/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef _TD_TRAFT_NODE_ID_H_
#define _TD_TRAFT_NODE_ID_H_

#ifdef __cplusplus
extern "C" {
#endif

typedef uint16_t raft_node_id_t;

#define RAFT_NODE_ID_NONE UINT16_MAX

// Global node id
extern raft_node_id_t raftNodeID;

#define RAFT_SELF_NODE_ID() raftNodeID
#define RAFT_NODE_ID_IS_NONE(id) ((id) == RAFT_NODE_ID_NONE)

#ifdef __cplusplus
}
#endif

#endif /*_TD_TRAFT_NODE_ID_H_*/