/* Copyright (c) 2025, ctrip.com
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "server.h"
#include <gtid.h>
#include <ctype.h>


rdbSaveInfoGtid *rdbSaveInfoGtidCreate() {
    rdbSaveInfoGtid *gtid_rsi = zcalloc(sizeof(rdbSaveInfoGtid));
    return gtid_rsi;
}

void rdbSaveInfoGtidDestroy(rdbSaveInfoGtid *gtid_rsi) {
    if (gtid_rsi == NULL) return;
    if (gtid_rsi->gtid_executed) {
        gtidSetFree(gtid_rsi->gtid_executed);
        gtid_rsi->gtid_executed = NULL;
    }
    if (gtid_rsi->gtid_lost) {
        gtidSetFree(gtid_rsi->gtid_lost);
        gtid_rsi->gtid_lost = NULL;
    }
    zfree(gtid_rsi);
}

#define GTID_AUX_REPL_MODE    "gtid-repl-mode"
#define GTID_AUX_EXECUTED     "gtid-executed"
#define GTID_AUX_LOST         "gtid-lost"

int rdbSaveInfoAuxFieldsGtid(rio* rdb, rdbSaveInfo *rsi) {
    char *repl_mode = NULL;
    sds gtid_executed_repr = NULL, gtid_lost_repr = NULL;

    /* No need to save gtid related rep stream info if rdb is not in any
     * kind of replication history */
    if (rsi == NULL
#ifdef ENABLE_SWAP
            || rsi->rsi_is_null
#endif
       ) return 1;

    repl_mode = (char*)replModeName(server.repl_mode->mode);
    gtid_executed_repr = gtidSetDump(server.gtid_executed);
    gtid_lost_repr = gtidSetDump(server.gtid_lost);

    /* Note: gtid-repl-mode must save before other gtid aux fields, otherwise
     * aux fields will lost when load because gtid save info not initiated. */
    if (rdbSaveAuxField(rdb, GTID_AUX_REPL_MODE, strlen(GTID_AUX_REPL_MODE),
                repl_mode, strlen(repl_mode)) == 0) {
        goto err;
    }

    if (rdbSaveAuxField(rdb, GTID_AUX_EXECUTED,
                strlen(GTID_AUX_EXECUTED),
                gtid_executed_repr, sdslen(gtid_executed_repr)) == -1) {
        goto err;
    }

    if (rdbSaveAuxField(rdb, GTID_AUX_LOST,
                strlen(GTID_AUX_LOST),
                gtid_lost_repr, sdslen(gtid_lost_repr)) == -1) {
        goto err;
    }

    sdsfree(gtid_executed_repr);
    sdsfree(gtid_lost_repr);
    return 1;

err:
    sdsfree(gtid_executed_repr);
    sdsfree(gtid_lost_repr);
    return -1;
}

int loadInfoAuxFieldsGtid(robj* key, robj* val, rdbSaveInfo *rsi) {
    rdbSaveInfoGtid *gtid_rsi = NULL;
    if (rsi) gtid_rsi = rsi->gtid;

    if (!strcasecmp(key->ptr, GTID_AUX_REPL_MODE)) {
        serverAssert(gtid_rsi == NULL);
        int repl_mode;

        if (!strcasecmp(val->ptr, "xsync"))
            repl_mode = REPL_MODE_XSYNC;
        else
            repl_mode = REPL_MODE_PSYNC;

        if (rsi) {
            rsi->gtid = rdbSaveInfoGtidCreate();
            rsi->gtid->repl_mode = repl_mode;
        }
        return 1;
    } else if (!strcasecmp(key->ptr, GTID_AUX_EXECUTED)) {
        if (rsi) {
            serverAssert(gtid_rsi);
            gtid_rsi->gtid_executed = gtidSetDecode(val->ptr,sdslen(val->ptr));
        }
        return 1;
    } else if (!strcasecmp(key->ptr, GTID_AUX_LOST)) {
        if (rsi) {
            serverAssert(gtid_rsi);
            gtidSet *gtid_lost = gtidSetDecode(val->ptr, sdslen(val->ptr));
            if (gtid_lost == NULL) gtid_lost = gtidSetNew();
            gtid_rsi->gtid_lost = gtid_lost;
        }
        return 1;
    }
    return 0;
}

