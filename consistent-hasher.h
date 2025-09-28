//////////////////////////////////////////////////////////////////////
// SPDX-License-Identifier: MIT
//
// consistent-hasher.h
// --------------------
//
// A lightweight, header-only implementation of consistent hashing in
// C99.
//
//
// Author:  Giovanni Santini
// Mail:    giovanni.santini@proton.me
// License: MIT
//
//
// Documentation
// -------------
//
// Consistent hashing is a technique for distributing keys across a
// dynamic set of nodes (e.g., servers or caches). Keys and nodes are
// both hashed onto a circular "ring," and each key is assigned to the
// nearest node clockwise on the ring. When nodes are added or
// removed, only a small fraction of keys need to be remapped, unlike
// in traditional modulo-based hashing.
//
// Benefits:
//   - Minimizes data movement when scaling the number of nodes
//   - Provides fault tolerance in distributed systems
//   - Enables better load distribution when combined with virtual nodes
//
// This library implements functionalities to add and remove nodes,
// and to get the closest node from an item. You can use this inside
// your hashmap logic to move only NUM_ITEMS / NUM_NODES items when
// resizing.
//
// Additional info:
// - https://en.wikipedia.org/wiki/Consistent_hashing
//
//
// Usage
// ----
//
// Do this:
//
//   #define CONSISTENT_HASHER_IMPLEMENTATION
//
// before you include this file in *one* C or C++ file to create the
// implementation.
//
// i.e. it should look like this:
//
//   #include ...
//   #include ...
//   #include ...
//   #define CONSISTENT_HASHER_IMPLEMENTATION
//   #include "consistent-hasher.h"
//
// You can tune the library by #defining certain values. See the
// "Config" comments under "Configuration" below.
//
//
// Code
// ----
//
// The official git repository of consistent-hasher.h is hosted at:
//
//     https://github.com/San7o/consistent-hasher.h
//
// This is part of a bigger collection of header-only C99 libraries
// called "micro-headers", contributions are welcome:
//
//     https://github.com/San7o/micro-headers
//

#ifndef _CONSISTENT_HASHER_H_
#define _CONSISTENT_HASHER_H_

#define CONSISTENT_HASHER_MAJOR 0
#define CONSISTENT_HASHER_MINOR 1

#ifdef __cplusplus
extern "C" {
#endif

//
// Configuration
//

// Config: the type of the hash
#ifndef CONSISTENT_HASHER_HASH
  #define CONSISTENT_HASHER_HASH unsigned int
#endif

// Config: initial allocation capacity
#ifndef CONSISTENT_HASHER_INITIAL_CAPACITY
  #include <stdlib.h>
  #define CONSISTENT_HASHER_INITIAL_CAPACITY 8
#endif

// Config: memory allocator
// Note: this will be called like calloc(3)
#ifndef CONSISTENT_HASHER_CALLOC
  #include <stdlib.h>
  #define CONSISTENT_HASHER_CALLOC calloc
#endif

// Config: free memory
// NoteL this will be called like free(3)
#ifndef CONSISTENT_HASHER_FREE
  #include <stdlib.h>
  #define CONSISTENT_HASHER_FREE free
#endif

//
// Types
//

#include <stdbool.h>

typedef CONSISTENT_HASHER_HASH ConsistentHasherHash;

// Errors
typedef enum {
  CONSISTENT_HASHER_OK = 0,
  CONSISTENT_HASHER_ERROR_IS_NULL,
  CONSISTENT_HASHER_ERROR_ALLOCATION,
  CONSISTENT_HASHER_ERROR_NODE_PRESENT,
  _CONSISTENT_HASHER_ERROR_MAX,
} ConsistentHasherError;

// A node
typedef struct {
  // The hash of the node
  ConsistentHasherHash hash;
  // Position in the ring buffer
  unsigned int position;
} ConsistentHasherNode;

// The ConsistentHasher
typedef struct {
  // Size of the ring buffer
  unsigned int ring_size;
  // Dynamic sorted array of nodes
  ConsistentHasherNode *nodes;
  // Number of nodes present in the array
  int nodes_len;
  // Allocated memory in the dynamic array
  int nodes_capacity;
} ConsistentHasher;

//
// Function Declarations
//

// Initialize [ch] with [ring_size] slots
//
// Notes: Remember to destroy [ch] when you are done.
void consistent_hasher_init(ConsistentHasher *ch,
                            unsigned int ring_size);

// Free allocated memory in [ch]
void consistent_hasher_destroy(ConsistentHasher *ch);

// Insert a node with [node_hash] in [ch]
//
// Returns: CONSISTENT_HASHER_OK on success, or an error otherwise
// Note: Fails if trying to insert a [node_hash] that is already
// present
ConsistentHasherError
consistent_hasher_insert_node(ConsistentHasher *ch,
                              ConsistentHasherHash node_hash);

// Remove node with [node_hash] in [ch]
//
// Returns: CONSISTENT_HASHER_OK on success, or an error otherwise
ConsistentHasherError
consistent_hasher_delete_node(ConsistentHasher *ch,
                              ConsistentHasherHash node_hash);

// Get the hash of the node corresponding to [item_hash] in [ch]
ConsistentHasherHash
consistent_hasher_get_node_of(ConsistentHasher *ch,
                              ConsistentHasherHash item_hash);
  
//
// Implementations
//

#ifdef CONSISTENT_HASHER_IMPLEMENTATION


void consistent_hasher_init(ConsistentHasher *ch,
                            unsigned int ring_size)
{
  if (!ch) return;

  *ch = (ConsistentHasher) {
    .ring_size = ring_size,
    .nodes_len = 0,
    .nodes_capacity = 0,
    .nodes = NULL,
  };
  
  return;
}
 
void consistent_hasher_destroy(ConsistentHasher *ch)
{
  if (!ch) return;
  
  if (ch->nodes) CONSISTENT_HASHER_FREE(ch->nodes);
  ch->nodes = NULL;
  
  return;
}

bool _consistent_hasher_binary_search(ConsistentHasher *ch,
                                      ConsistentHasherHash node_hash,
                                      int *index)
{
  int start = 0;
  int end = ch->nodes_len - 1;
  unsigned int position = node_hash % ch->ring_size;
  int mid = (end - start) / 2 + start;
  
  while(end >= start) {
    
    if (ch->nodes[mid].position == position)
    {
      if (index) *index = start;
      return true;
    }
    
    if (ch->nodes[mid].position > position)
    {
      end = mid - 1;
    }
    else if (ch->nodes[mid].position < position)
    {
      start = mid + 1;
    }

    mid = (end - start) / 2 + start;
  }

  if (index) *index = mid;
  return false;
}

ConsistentHasherError
consistent_hasher_insert_node(ConsistentHasher *ch,
                              ConsistentHasherHash node_hash)
{
  if (!ch) return CONSISTENT_HASHER_ERROR_IS_NULL;

  ConsistentHasherNode new_node = (ConsistentHasherNode) {
    .hash = node_hash,
    .position = node_hash % ch->ring_size,
  };
  
  if (!ch->nodes)
  {
    ch->nodes = CONSISTENT_HASHER_CALLOC(CONSISTENT_HASHER_INITIAL_CAPACITY,
                                         sizeof(ConsistentHasherNode));
    if (!ch->nodes) return CONSISTENT_HASHER_ERROR_ALLOCATION;

    ch->nodes_capacity = CONSISTENT_HASHER_INITIAL_CAPACITY;
    ch->nodes[0] = new_node;
    ch->nodes_len = 1;
    goto done;
  }

  int index;
  bool found = _consistent_hasher_binary_search(ch, node_hash, &index);
  if (found) return CONSISTENT_HASHER_ERROR_NODE_PRESENT;
  
  if (ch->nodes_capacity == ch->nodes_len)
  {
    unsigned int new_capacity = ch->nodes_capacity * 2;
    ConsistentHasherNode *new_nodes =
      CONSISTENT_HASHER_CALLOC(new_capacity,
                               sizeof(ConsistentHasherNode));
    if (!new_nodes) return CONSISTENT_HASHER_ERROR_ALLOCATION;

    for (int i = 0; i < ch->nodes_capacity; ++i)
    {
      new_nodes[ (i >= index && i < ch->nodes_capacity) ? i+1 : i ] =
        ch->nodes[i];
    }
    CONSISTENT_HASHER_FREE(ch->nodes);

    ch->nodes = new_nodes;
    ch->nodes_capacity = new_capacity;
    ch->nodes[index] = new_node;
    ch->nodes_len += 1;
    goto done;
  }

  for (int i = ch->nodes_len - 1; i > index + 1; --i)
  {
    ch->nodes[i] = ch->nodes[i - 1];
  }
  ch->nodes[index] = new_node;
  ch->nodes_len += 1;
  
 done:
  return CONSISTENT_HASHER_OK;
}

ConsistentHasherError
consistent_hasher_delete_node(ConsistentHasher *ch,
                              ConsistentHasherHash node_hash)
{
  if (!ch) return CONSISTENT_HASHER_ERROR_IS_NULL;

  int index;
  bool found = _consistent_hasher_binary_search(ch, node_hash, &index);
  if (!found)
  {
    goto done;
  }

  if (ch->nodes_len - 1 == ch->nodes_capacity / 2)
  {
    ConsistentHasherNode *new_nodes =
      CONSISTENT_HASHER_CALLOC(ch->nodes_len - 1,
                             sizeof(ConsistentHasherNode));
    if (!new_nodes) return CONSISTENT_HASHER_ERROR_ALLOCATION;
    
    for (int i = 0; i < ch->nodes_len - 1; ++i)
    {
      new_nodes[i] = ch->nodes[ (i < index) ? i : i+1 ];
    }
    CONSISTENT_HASHER_FREE(ch->nodes);
    
    ch->nodes = new_nodes;
    ch->nodes_len = ch->nodes_len - 1;
    ch->nodes_capacity = ch->nodes_len;
    goto done;
  }

  for (int i = index; i < ch->nodes_len - 2; ++i)
  {
    ch->nodes[i] = ch->nodes[i+1];
  }
  ch->nodes_len = ch->nodes_len - 1;
  
 done:
  return CONSISTENT_HASHER_OK;
}

ConsistentHasherHash
consistent_hasher_get_node_of(ConsistentHasher *ch,
                              ConsistentHasherHash item_hash)
{
  int index;
  _consistent_hasher_binary_search(ch, item_hash, &index);
  if (item_hash % ch->ring_size > ch->nodes[index].position)
    index = 0;
  
  return ch->nodes[index].hash;
}
  
#endif // CONSISTENT_HASHER_IMPLEMENTATION

//
// Examples
//

#if 0

#define CONSISTENT_HASHER_IMPLEMENTATION
#include "consistent-hasher.h"

#define RING_SIZE 1024

int main(void)
{
  ConsistentHasher ch;
  consistent_hasher_init(&ch, RING_SIZE);
  
  consistent_hasher_insert_node(&ch, 123);
  consistent_hasher_insert_node(&ch, 456);
  
  assert(consistent_hasher_get_node_of(&ch, 100) == 123);

  consistent_hasher_destroy(&ch);
  return 0;
}

#endif // 0

  
#ifdef _cplusplus
}
#endif

#endif // _CONSISTENT_HASHER_H_
