// SPDX-License-Identifier: MIT

#define CONSISTENT_HASHER_INITIAL_CAPACITY 1
#define CONSISTENT_HASHER_IMPLEMENTATION
#include "consistent-hasher.h"

#include <stdio.h>
#include <assert.h>

#define RING_SIZE 1024

int main(void)
{
  ConsistentHasher ch;
  consistent_hasher_init(&ch, RING_SIZE);
  
  assert(consistent_hasher_insert_node(&ch, 123) == CONSISTENT_HASHER_OK);
  assert(consistent_hasher_insert_node(&ch, 456) == CONSISTENT_HASHER_OK);
  assert(consistent_hasher_insert_node(&ch, 924) == CONSISTENT_HASHER_OK);
  assert(consistent_hasher_insert_node(&ch, 123) ==
         CONSISTENT_HASHER_ERROR_NODE_PRESENT);

  /*
  // Debug prints
  printf("ch.nodes[0].position: %d\n", ch.nodes[0].position);
  printf("ch.nodes[1].position: %d\n", ch.nodes[1].position);
  printf("ch.nodes[2].position: %d\n", ch.nodes[2].position);
  printf("ch.nodes_len: %d\n", ch.nodes_len);
  */
  
  assert(consistent_hasher_delete_node(&ch, 123) == CONSISTENT_HASHER_OK);  
  assert(consistent_hasher_insert_node(&ch, 123) == CONSISTENT_HASHER_OK);
  
  assert(consistent_hasher_get_node_of(&ch, 123) == 123);
  assert(consistent_hasher_get_node_of(&ch, 100) == 123);
  assert(consistent_hasher_get_node_of(&ch, 90) == 123);
  assert(consistent_hasher_get_node_of(&ch, 150) == 456);
  assert(consistent_hasher_get_node_of(&ch, 400) == 456);
  assert(consistent_hasher_get_node_of(&ch, 457) == 924);
  assert(consistent_hasher_get_node_of(&ch, 800) == 924);
  assert(consistent_hasher_get_node_of(&ch, 1000) == 123);

  consistent_hasher_destroy(&ch);
  return 0;
}
