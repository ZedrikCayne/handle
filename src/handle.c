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

#include "handle.h"

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

bool forwardThread(struct CS_Thread *myThread, int threadState, void *context) {
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

bool forward( struct CS_ClientInfo *info, int portNum ) {
    struct CS_Thread *pRemoteThread = NULL;
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

    pRemoteThread = CS_threadStart("Remote", fullContext, forwardThread );

    //This thread is the one that sucks in from the request,
    //and shoves directly out to the remote.
    //
    //The other thread will eat from the remote and shove out to the request source.
    do {
        int32_t moved = CS_PP_moveBuffer( info->buffer, CS_socketLockOutputBuffer( fullContext->socket ) );
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
    CS_threadReturn( pRemoteThread );

CLEANUP:
    if( reply ) CS_httpCloseRequest( reply );
    if( fullContext ) CS_free( fullContext );
    fullContext = NULL;
    return true;
}
