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
struct socket_data {
    struct CS_Socket *cs_socket;
};

struct reply_data {
    struct CS_Reply *cs_reply;
};

struct client_info_data {
    struct CS_ClientInfo *cs_client;
    struct CS_ReplyInfo *cs_reply;
};


bool forward( struct CS_ClientInfo *info, int portNum );

struct forwardContext {
    struct CS_Mutex *mutex;
    struct CS_Thread *thread;
    struct CS_ClientInfo *info;
    struct CS_Socket *socket;
    struct CS_RequestReply *reply;
    bool clientClosed;
    bool serverClosed;
}; 

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
                    return true;
                }
                int bytesToServer = CS_serverWriteOutputBuffer( fwd->info );
                if( bytesToServer <= 0 ) {
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

bool forward8080( struct CS_ClientInfo *info ) {
    return forward(info,8080);
}

bool forward8081( struct CS_ClientInfo *info ) {
    return forward(info,8081);
}

bool forward8082( struct CS_ClientInfo *info ) {
    return forward(info,8082);
}

bool forward8083( struct CS_ClientInfo *info ) {
    return forward(info,8083);
}

bool forward8084( struct CS_ClientInfo *info ) {
    return forward(info,8084);
}

bool forward8085( struct CS_ClientInfo *info ) {
    return forward(info,8085);
}

bool forward8086( struct CS_ClientInfo *info ) {
    return forward(info,8086);
}

bool forward8087( struct CS_ClientInfo *info ) {
    return forward(info,8087);
}

#define SOCKET_BUFFER_SIZE 8192

bool relayForward( struct CS_ClientInfo *info, int portNum ) {
    struct CS_RequestReply *reply = NULL;
    struct forwardContext *fullContext = CS_allocZero( sizeof(struct forwardContext) );

    if( CS_serverGetRequestHeader(info, &CS_STRING("X-Forwarded-For") ) != NULL ) { 
        CS_serverReplyError( info, 421, "Edge server. Must be first in forward chain." );
        return true;
    }
    if( CS_serverGetRequestHeader(info, &CS_STRING("X-Real-IP")) != NULL ) {
        CS_serverReplyError( info, 421, "Edge server. Must be first in forward chain." );
        return true;
    }
    
    if( fullContext == NULL ) {
        CS_serverReplyError( info, CS_RESPONSE_500, "OOM forwarding" );
        goto CLEANUP;
    }

    const struct CS_String *networkAddress = CS_networkAddressToTempString( &info->clientSocketAddress );
    CS_serverAddOrReplaceRequestHeader( info, &CS_STRING("X-Real-IP"), networkAddress );
    CS_serverAddOrReplaceRequestHeader( info, &CS_STRING("X-Forwarded-For"), networkAddress );
    CS_serverAddOrReplaceRequestHeader( info, &CS_STRING("X-Forwarded-Proto"), info->ssl?&CS_STRING("https"):&CS_STRING("http") );

    struct CS_RequestInfo *requestInfo = &info->requestInfo;

    //If we've got a num form parameters, we need to remove the content.
    if( requestInfo->numFormParameters != 0 ) {
        CS_serverRemoveRequestHeader(info, &CS_STRING("Content-Length"));
    }

    reply = CS_httpStartRequest(
            CS_httpStringToMethodEnum( &requestInfo->method ),
            CS_stringTempSnprintf(1024, "http://127.0.0.1:%d%.*s", portNum, requestInfo->uri.length, requestInfo->uri.data ),
            requestInfo->headers, 
            requestInfo->numHeaders,
            requestInfo->parameters,
            requestInfo->numParameters,
            requestInfo->formParameters,
            requestInfo->numFormParameters,
            NULL, 0, NULL );

    if( reply == NULL ) {
        CS_serverReplyError( info, CS_RESPONSE_403, "Remote not responding" );
        return true;
    }

    if( reply->responseEnum == CS_RESPONSE_101 ) {
        //We're hitting one of them websockets. Spin it up!
        fullContext->reply = reply;
        fullContext->info = info;
        struct CS_Thread *remoteThread = CS_threadStart("Remote", fullContext, forwardThreadCSSocket );
        do {
            int outgoing = CS_httpPushBufferToRemote( reply, info->buffer );
            if( outgoing < 0 ) break;
            int incoming = CS_serverFillIncomingBuffer( info );
            if( incoming < 0 ) {
                fullContext->serverClosed = true;
                break;
            }
        } while(true);
        CS_mutexLock( fullContext->mutex );
        CS_mutexUnlock( fullContext->mutex );
        CS_mutexReturn( fullContext->mutex );
        CS_threadReturn( remoteThread );
        return true;
    } else {
    }

CLEANUP:
    return false;
}

bool forward( struct CS_ClientInfo *info, int portNum ) {
    struct CS_Thread *remoteThread = NULL;
    struct CS_RequestReply *reply = NULL;
    struct forwardContext *fullContext = CS_allocZero( sizeof(struct forwardContext) );

    if( fullContext == NULL ) {
        CS_serverReplyError( info, CS_RESPONSE_500, "OOM forwarding" );
        goto CLEANUP;
    }

    if( CS_PP_toothpaste( info->buffer, info->requestInfo.headerSize ) ) {
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
    fullContext->info = info;

    remoteThread = CS_threadStart("Remote", fullContext, forwardThreadCSSocket );

    //This thread is the one that sucks in from the request,
    //and shoves directly out to the remote.
    //
    //The other thread will eat from the remote and shove out to the request source.
    do {
        CS_PP_moveBuffer( info->buffer, CS_socketLockOutputBuffer( fullContext->socket ) );
        CS_socketUnlockOutputBuffer( fullContext->socket );
        int outgoing = CS_socketEmptyOutputBuffer( fullContext->socket, false );
        if( outgoing < 0 ) break;
        int incoming = CS_serverFillIncomingBuffer( info );
        if( incoming < 0 ) break;
    } while(true);

    //Close the pass through socket. (Might be closed already)
    CS_socketClose( fullContext->socket );

    CS_mutexLock( fullContext->mutex );
    CS_mutexUnlock( fullContext->mutex );
    CS_mutexReturn( fullContext->mutex );
    CS_threadReturn( remoteThread );

CLEANUP:
    if( reply ) CS_httpCloseRequest( reply );
    if( fullContext ) CS_free( fullContext );
    fullContext = NULL;
    return true;
}
