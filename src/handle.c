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
    struct CS_RequestReply *reply;
    bool clientClosed;
    bool serverClosed;
}; 

bool forwardThread(struct CS_Thread *myThread, int threadState, void *context) {
    struct forwardContext *fwd = (struct forwardContext *)context;

    switch( threadState ) {
    case CS_THREAD_START:
        CS_mutexLock( fwd->mutex );
    case CS_THREAD_RUNNING:
        if( fwd->serverClosed ) return true;
        {
            int bytesInFromRemote = CS_httpFillReplyFromRemote( fwd->reply );
            if( bytesInFromRemote <= 0 ) {
                return true;
            }
            while( CS_PP_dataSize(fwd->reply->buffer) > 0 ) {
                CS_PP_moveBuffer( fwd->reply->buffer, fwd->info->output );
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


bool forward( struct CS_ClientInfo *info, int portNum ) {
    struct CS_Thread *pRemoteThread = NULL;
    struct CS_RequestReply *reply = NULL;
    struct forwardContext *fullContext = CS_allocZero( sizeof(struct forwardContext) );

    if( fullContext == NULL ) {
        CS_serverReplyError( info, CS_RESPONSE_500, "OOM forwarding" );
        goto CLEANUP;
    }
    const struct CS_String *tbuff = CS_stringTempSnprintf( 2048, "http://127.0.0.1:%d%.*s", portNum, info->requestInfo.uri.length, info->requestInfo.uri.data );

    CS_serverRemoveRequestHeader( info, &CS_STRING("Content-Length") );
    //Fire off the request to where we are forwarding it to.
    reply = CS_httpStartRequest( info->requestInfo.requestMethodEnum,
            tbuff,
            info->requestInfo.headers, info->requestInfo.numHeaders, 
            info->requestInfo.parameters, info->requestInfo.numParameters,
            info->requestInfo.formParameters, info->requestInfo.numFormParameters,
            NULL, 0, NULL );

    if( reply == NULL ) {
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
        int outgoing = CS_httpPushBufferToRemote( reply, info->buffer );
        if( outgoing < 0 ) break;
        int incoming = CS_serverFillIncomingBuffer( info );
        if( incoming < 0 ) break;
    } while(true);

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
