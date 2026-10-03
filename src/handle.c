#include <string.h>
#include <stdbool.h>

#include <crankshaft/hashtable.h>
#include <crankshaft/server.h>
#include <crankshaft/socket.h>
#include <crankshaft/websocket.h>
#include <crankshaft/html.h>
#include <crankshaft/mime.h>
#include <crankshaft/json.h>
#include <crankshaft/logger.h>
#include <crankshaft/list.h>
#include <crankshaft/util.h>
#include <crankshaft/base64.h>
#include <crankshaft/thread.h>
#include <crankshaft/mutex.h>
#include <crankshaft/alloc.h>
#include <crankshaft/tempbuff.h>
#include <crankshaft/network.h>
#include <crankshaft/pipe.h>

#include "handle.h"

//Pipe segments...pipe entry for 'CS_Socket in/out' and 'CS_Reply in/out' and
//CS_ClientInfo in/out
struct forwardContext {
    struct CS_Mutex *mutex;
    struct CS_Thread *thread;
    struct CS_ClientInfo *info;
    struct CS_Socket *socket;
    struct CS_RequestReply *reply;
    struct CS_Pipe *clientToServer;
    struct CS_Pipe *severToClient;
    bool clientClosed;
    bool serverClosed;
}; 

static bool __create( struct CS_Pipe *currentSection, const void *data ) {
    currentSection->pipeData = (void *)data;
    return true;
}
static bool __close( struct CS_Pipe *currentSection ) {
    return true;
}
static int32_t _process_reply_in( struct CS_Pipe *currentSection ) {
    struct forwardContext *fwd = (struct forwardContext *)currentSection->pipeData;
    int32_t bytesFilled = CS_httpFillReplyFromRemote( fwd->reply );
    return bytesFilled;
}
static int32_t _process_reply_out( struct CS_Pipe *currentSection ) {
    struct forwardContext *fwd = (struct forwardContext *)currentSection->pipeData;
    struct CS_PushPullBuffer *pp = CS_pipeNearestInBuffer( currentSection );
    if( !pp ) return true;
    int outgoing = CS_httpPushBufferToRemote( fwd->reply, pp );
    return outgoing;
}
static int32_t _process_server_in( struct CS_Pipe *currentSection ) {
    struct forwardContext *fwd = (struct forwardContext *)currentSection->pipeData;
    int incoming = CS_serverFillIncomingBuffer( fwd->info );
    currentSection->buffer = fwd->info->buffer;
    return incoming;
}
static int32_t _process_server_out( struct CS_Pipe *currentSection ) {
    struct forwardContext *fwd = (struct forwardContext *)currentSection->pipeData;
    struct CS_PushPullBuffer *pp = CS_pipeNearestInBuffer( currentSection );
    int32_t totalOut = 0;
    while( CS_PP_dataSize(pp) > 0 ) {
        if( CS_PP_moveBuffer( pp, fwd->info->output ) < 0 ) {
            return -1;
        }
        int bytesToServer = CS_serverWriteOutputBuffer( fwd->info );
        if( bytesToServer < 0 ) return true;
        totalOut += bytesToServer;
    }
    return totalOut;
}
static int32_t _process_socket_in( struct CS_Pipe *currentSection ) {
    struct forwardContext *fwd = (struct forwardContext *)currentSection->pipeData;
    int32_t bytesFilled = CS_socketFillIncomingBuffer( fwd->socket, false );
    currentSection->buffer = CS_socketLockInputBuffer( fwd->socket );
    CS_socketUnlockInputBuffer( fwd->socket );
    return bytesFilled;
}
static int32_t _process_socket_out( struct CS_Pipe *currentSection ) {
    struct forwardContext *fwd = (struct forwardContext *)currentSection->pipeData;
    struct CS_PushPullBuffer *pp = CS_pipeNearestInBuffer( currentSection );
    currentSection->buffer = CS_socketLockOutputBuffer( fwd->socket );
    if( CS_PP_moveBuffer( pp, currentSection->buffer ) < 0 ) return -1;
    CS_socketUnlockOutputBuffer( fwd->socket );
    return CS_socketEmptyOutputBuffer( fwd->socket, false );
}

struct CS_PipeDefinition _CS_PIPE_FWD_SERVER_IN = {
    CS_PIPE_FLAG_NO_BUFFER|CS_PIPE_FLAG_REQUIRE_DATA, _process_server_in, __close, __create
};
struct CS_PipeDefinition *CS_PIPE_FWD_SERVER_IN = &_CS_PIPE_FWD_SERVER_IN;
struct CS_PipeDefinition _CS_PIPE_FWD_SERVER_OUT = {
    CS_PIPE_FLAG_NO_BUFFER|CS_PIPE_FLAG_REQUIRE_DATA, _process_server_out, __close, __create
};
struct CS_PipeDefinition *CS_PIPE_FWD_SERVER_OUT = &_CS_PIPE_FWD_SERVER_OUT;
struct CS_PipeDefinition _CS_PIPE_FWD_SOCKET_IN = {
    CS_PIPE_FLAG_NO_BUFFER|CS_PIPE_FLAG_REQUIRE_DATA, _process_socket_in, __close, __create
};
struct CS_PipeDefinition *CS_PIPE_FWD_SOCKET_IN = &_CS_PIPE_FWD_SOCKET_IN;
struct CS_PipeDefinition _CS_PIPE_FWD_SOCKET_OUT = {
    CS_PIPE_FLAG_NO_BUFFER|CS_PIPE_FLAG_REQUIRE_DATA, _process_socket_out, __close, __create
};
struct CS_PipeDefinition *CS_PIPE_FWD_SOCKET_OUT = &_CS_PIPE_FWD_SOCKET_OUT;
struct CS_PipeDefinition _CS_PIPE_FWD_REPLY_IN = {
    CS_PIPE_FLAG_NO_BUFFER|CS_PIPE_FLAG_REQUIRE_DATA, _process_reply_in, __close, __create
};
struct CS_PipeDefinition *CS_PIPE_FWD_REPLY_IN = &_CS_PIPE_FWD_REPLY_IN;
struct CS_PipeDefinition _CS_PIPE_FWD_REPLY_OUT = {
    CS_PIPE_FLAG_NO_BUFFER|CS_PIPE_FLAG_REQUIRE_DATA, _process_reply_out, __close, __create
};
struct CS_PipeDefinition *CS_PIPE_FWD_REPLY_OUT = &_CS_PIPE_FWD_REPLY_OUT;

bool forward( struct CS_RequestInfo *info, int portNum );

bool forwardThreadReply(struct CS_Thread *myThread, int threadState, void *context) {
    struct forwardContext *fwd = (struct forwardContext *)context;
    switch( threadState ) {
    case CS_THREAD_START:
        CS_mutexLock( fwd->mutex );
        break;
    case CS_THREAD_RUNNING:
        if( fwd->serverClosed ) {
            return true;
        } else {
            int32_t bytesFilled = CS_httpFillReplyFromRemote( fwd->reply );
            if( bytesFilled <= 0 ) {
                //Our socket is closed or had some error...flow it back up. Might be closed
                //already.
                CS_serverKillClientSocket( fwd->info );
                return true;
            }
            struct CS_PushPullBuffer *ppIncoming = fwd->reply->buffer;
            while( CS_PP_dataSize(ppIncoming) > 0 ) {
                if( CS_PP_moveBuffer( ppIncoming, fwd->info->output ) < 0 ) {
                    CS_serverKillClientSocket( fwd->info );
                    return true;
                }
                int bytesToServer = CS_serverWriteOutputBuffer( fwd->info );
                if( bytesToServer <= 0 ) {
                    CS_serverKillClientSocket( fwd->info );
                    return true;
                }
            }
        }

        break;
    case CS_THREAD_STOP:
        CS_mutexUnlock( fwd->mutex );
        break;
    }
    return false;
}

bool forwardThreadCSSocket(struct CS_Thread *myThread, int threadState, void *context) {
    struct forwardContext *fwd = (struct forwardContext *)context;

    switch( threadState ) {
    case CS_THREAD_START:
        CS_mutexLock( fwd->mutex );
        break;
    case CS_THREAD_RUNNING:
        if( fwd->serverClosed ) {
            return true;
        } else
        {
            int32_t bytesFilled = CS_socketFillIncomingBuffer( fwd->socket, false );
            if( bytesFilled <= 0 ) {
                //Our socket is closed or had some error...flow it back up. Might be closed
                //already.
                CS_serverKillClientSocket( fwd->info );
                return true;
            }
            struct CS_PushPullBuffer *ppIncoming = CS_socketLockInputBuffer( fwd->socket );
            while( CS_PP_dataSize(ppIncoming) > 0 ) {
                int32_t moved = CS_PP_moveBuffer( ppIncoming, fwd->info->output );
                int bytesToServer = CS_serverWriteOutputBuffer( fwd->info );
                if( bytesToServer <= 0 ) {
                    CS_socketUnlockInputBuffer( fwd->socket );
                    CS_serverKillClientSocket( fwd->info );
                    return true;
                }
            }
            CS_socketUnlockInputBuffer( fwd->socket );
        }

        break;
    case CS_THREAD_STOP:
        CS_mutexUnlock( fwd->mutex );
        break;
    }
    return false;
}

bool forward8080( struct CS_RequestInfo *info ) {
    return forward(info,8080);
}

bool forward8081( struct CS_RequestInfo *info ) {
    return forward(info,8081);
}

bool forward8082( struct CS_RequestInfo *info ) {
    return forward(info,8082);
}

bool forward8083( struct CS_RequestInfo *info ) {
    return forward(info,8083);
}

bool forward8084( struct CS_RequestInfo *info ) {
    return forward(info,8084);
}

bool forward8085( struct CS_RequestInfo *info ) {
    return forward(info,8085);
}

bool forward8086( struct CS_RequestInfo *info ) {
    return forward(info,8086);
}

bool forward8087( struct CS_RequestInfo *info ) {
    return forward(info,8087);
}

#define SOCKET_BUFFER_SIZE 8192

bool relayForward( struct CS_RequestInfo *info, int portNum ) {
    bool returnValue = false;
    struct CS_RequestReply *reply = NULL;
    struct forwardContext *fullContext = NULL;
    struct CS_Pipe *outsideToUs = NULL;
    struct CS_Pipe *usToServer = NULL;
    struct CS_Pipe *serverToUs = NULL;
    struct CS_Pipe *usToOutside = NULL;

    if( CS_serverGetRequestHeader(info, &CS_STRING("X-Forwarded-For") ) != NULL ) { 
        CS_serverReplyError( info, 421, "Edge server. Must be first in forward chain." );
        returnValue = true;
        goto CLEANUP;
    }
    if( CS_serverGetRequestHeader(info, &CS_STRING("X-Real-IP")) != NULL ) {
        CS_serverReplyError( info, 421, "Edge server. Must be first in forward chain." );
        returnValue = true;
        goto CLEANUP;
    }
    
    fullContext = CS_allocZero( sizeof(struct forwardContext) );

    if( fullContext == NULL ) {
        CS_serverReplyError( info, CS_RESPONSE_500, "OOM forwarding" );
        returnValue = true;
        goto CLEANUP;
    }
    
    fullContext->mutex = CS_mutexTake();
    fullContext->info = info->clientInfo;

    const struct CS_String *networkAddress = CS_networkAddressToTempString( &info->clientInfo->clientSocketAddress );
    CS_serverAddOrReplaceRequestHeader( info, &CS_STRING("X-Real-IP"), networkAddress );
    CS_serverAddOrReplaceRequestHeader( info, &CS_STRING("X-Forwarded-For"), networkAddress );
    CS_serverAddOrReplaceRequestHeader( info, &CS_STRING("X-Forwarded-Proto"), info->clientInfo->ssl?&CS_STRING("https"):&CS_STRING("http") );

    //If we've got a num form parameters, we need to remove the content length
    if( info->numFormParameters != 0 ) {
        CS_serverRemoveRequestHeader(info, &CS_STRING("Content-Length"));
    }

    reply = CS_httpStartRequest(
            CS_httpStringToMethodEnum( &info->method ),
            CS_stringTempSnprintf(1024, "http://127.0.0.1:%d%.*s", portNum, info->uri.length, info->uri.data ),
            info->headers, 
            info->numHeaders,
            info->parameters,
            info->numParameters,
            info->formParameters,
            info->numFormParameters,
            NULL, 0, NULL );

    if( reply == NULL ) {
        CS_serverReplyError( info, CS_RESPONSE_403, "Remote not responding" );
        goto CLEANUP;
    }

    fullContext->reply = reply;
    
    //Create the IO pipes.
    outsideToUs = CS_pipeCreate( CS_PIPE_FWD_SERVER_IN, 0, fullContext );
    usToServer = CS_pipeCreate( CS_PIPE_FWD_REPLY_OUT, 0, fullContext );
    serverToUs = CS_pipeCreate( CS_PIPE_FWD_REPLY_IN, 0, fullContext );
    usToOutside = CS_pipeCreate( CS_PIPE_FWD_SERVER_OUT, 0, fullContext );
    
NEXT_MESSAGE:
   
    //We might need to finish the request off here. Especially if there was more
    //data to go.
    const struct CS_String *dataSize = CS_serverGetRequestHeader(info, &CS_STRING("Content-Length") );
    int32_t bytesToFinishRequest = 0;
    if( info->numFormParameters == 0 && dataSize != NULL ) {
        bytesToFinishRequest = CS_stringAtoi(dataSize);
    }
    const struct CS_String *encoding = CS_serverGetRequestHeader(info, &CS_STRING("Transfer-Encoding") );
    if( bytesToFinishRequest == 0 && encoding && CS_stringTempStrstr(encoding,&CS_STRING("chunked")) ) {
        bytesToFinishRequest = -1;
    }

    if( bytesToFinishRequest != 0 ) {
        if( bytesToFinishRequest < 0 ) {
            //If this is negative, we don't know the size, but we know it is chunked. So connect
            //a pipe that will stop when we've got all the chunk bits.
            struct CS_Pipe *encodeCopy = CS_pipeCreate( CS_PIPE_CHUNK_COPY, SOCKET_BUFFER_SIZE, NULL );
            if( encodeCopy == NULL ) {
                goto CLEANUP;
            }
            CS_pipeHook( outsideToUs, encodeCopy );
            CS_pipeHook( encodeCopy, usToServer );
        } else {
            //This is a positive number. Connect the pipe that will only transfer the number
            //of bytes we want.
            struct CS_Pipe *numBytesExtraPipe = CS_pipeCreate( CS_PIPE_LIMITED, SOCKET_BUFFER_SIZE, &bytesToFinishRequest );
            if( numBytesExtraPipe == NULL ) {
                goto CLEANUP;
            }
            CS_pipeHook( outsideToUs, numBytesExtraPipe );
            CS_pipeHook( numBytesExtraPipe, usToServer );
        }
        while( !CS_pipeDoneOrError( outsideToUs ) ) CS_pipeProcess( outsideToUs );
    }

    //The response should be finished by now. At least enough to have the 'reply'
    //bits done.


    

    if( reply->responseEnum == CS_RESPONSE_101 ) {
        //We're hitting one of them websockets. The remote connection is now a raw
        //pass-through, so wrap the reply's socket to feed the remote thread.
        fullContext->socket = CS_socketInit( reply->remoteSocket, portNum, reply->ssl,
                                            SOCKET_BUFFER_SIZE, 8192, false, false );
        if( fullContext->socket == NULL ) {
            CS_serverReplyError( info, CS_RESPONSE_500, "Cannot wrap remote socket." );
            goto CLEANUP;
        }
        //Kick off!
        struct CS_Thread *remoteThread = CS_threadStart("Remote", fullContext, forwardThreadCSSocket );
        if( remoteThread == NULL ) {
            CS_serverReplyError( info, CS_RESPONSE_500, "Cannot start remote thread." );
        } else {
            do {
                //Take the last dregs and shove it to the server.
                int outgoing = CS_httpPushBufferToRemote( reply, info->clientInfo->buffer );
                if( outgoing < 0 ) {
                    fullContext->serverClosed = true;
                    break;
                }
                //Queue up new stuff.
                int incoming = CS_serverFillIncomingBuffer( info->clientInfo );
                if( incoming < 0 ) {
                    fullContext->serverClosed = true;
                    break;
                }
            } while(true);
            CS_mutexLock( fullContext->mutex );
            CS_mutexUnlock( fullContext->mutex );
            CS_threadReturn( remoteThread );
            returnValue = true;
        }
    }
    else
    {
        //Craft a reply from the reply we got.
    }

CLEANUP:
    if( outsideToUs ) CS_pipeFree( outsideToUs );
    if( usToServer ) CS_pipeFree( usToServer );
    if( serverToUs ) CS_pipeFree( serverToUs );
    if( usToOutside ) CS_pipeFree( usToOutside );
    if( fullContext ) {
        if( fullContext->socket ) CS_socketDestroy( fullContext->socket );
        if( fullContext->mutex ) CS_mutexReturn( fullContext->mutex );
        CS_free( fullContext );
    }
    if( reply ) CS_httpCloseRequest( reply );
    return returnValue;
}

bool forward( struct CS_RequestInfo *info, int portNum ) {
    struct CS_Thread *remoteThread = NULL;
    struct CS_RequestReply *reply = NULL;
    struct forwardContext *fullContext = CS_allocZero( sizeof(struct forwardContext) );

    if( fullContext == NULL ) {
        CS_serverReplyError( info, CS_RESPONSE_500, "OOM forwarding" );
        goto CLEANUP;
    }

    if( CS_PP_toothpaste( info->clientInfo->buffer, info->headerSize ) ) {
        CS_serverReplyError( info, CS_RESPONSE_500, "Could not put the toothpaste back in the tube." );
        goto CLEANUP;
    }

    fullContext->socket = CS_socketConnect( "127.0.0.1", false, portNum, false, false, SOCKET_BUFFER_SIZE, 8192, false, false );

    if( fullContext->socket == NULL ) {
        CS_serverReplyError( info, CS_RESPONSE_403, "Remote not responding" );
        goto CLEANUP;
    }

    fullContext->mutex = CS_mutexTake();
    fullContext->reply = reply;
    fullContext->info = info->clientInfo;

    remoteThread = CS_threadStart("Remote", fullContext, forwardThreadCSSocket );

    //This thread is the one that sucks in from the request,
    //and shoves directly out to the remote.
    //
    //The other thread will eat from the remote and shove out to the request source.
    while(true) {
        CS_PP_moveBuffer( info->clientInfo->buffer, CS_socketLockOutputBuffer( fullContext->socket ) );
        CS_socketUnlockOutputBuffer( fullContext->socket );
        if( CS_socketEmptyOutputBuffer( fullContext->socket, false ) < 0 ) break;
        //Still holding data for the remote. Keep moving it instead of
        //reading more (the client buffer might be full, in which case a
        //read would return 0 without telling us why).
        if( CS_PP_dataSize( info->clientInfo->buffer ) > 0 ) continue;
        //0 is a clean EOF (client went away). Break on it, or we spin
        //forever and never return the request.
        if( CS_serverFillIncomingBuffer( info->clientInfo ) <= 0 ) break;
    }

    //Close the pass through socket. (Might be closed already)
    CS_socketClose( fullContext->socket );

    //The thread holds the mutex from START to STOP, so once we have it,
    //it is done with the context and safe to tear everything down.
    CS_mutexLock( fullContext->mutex );
    CS_mutexUnlock( fullContext->mutex );
    CS_threadReturn( remoteThread );

CLEANUP:
    if( reply ) CS_httpCloseRequest( reply );
    if( fullContext ) {
        if( fullContext->socket ) CS_socketDestroy( fullContext->socket );
        if( fullContext->mutex ) CS_mutexReturn( fullContext->mutex );
        CS_free( fullContext );
    }
    return true;
}
