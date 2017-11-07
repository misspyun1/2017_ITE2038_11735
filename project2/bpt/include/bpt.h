#ifndef __BPT_H__
#define __BPT_H__

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>
#include <fcntl.h>
#include <unistd.h>
#ifdef WINDOWS
#define bool char
#define false 0
#define true 1
#endif

#define PAGE_SIZE 4096
#define RECORD_SIZE 128
#define LEAF_ORDER 31
#define INTERNAL_ORDER 248

extern int fd;

int open_db (char * pathname);

int cut (int length);

void make_free_page (void);
int64_t make_leaf ( void );
int64_t start_new_tree (int64_t key);
int64_t make_internal ( void);


int get_left_index (int64_t parent, int64_t key);
int get_neighbor_index(int64_t n);

int64_t insert_into_parent(int64_t root, int64_t left, int64_t key, int64_t right);
int64_t insert_into_new_root (int64_t left, int64_t key, int64_t right);
int64_t insert_into_internal(int64_t root, int64_t parent, int left_index, int64_t key, int64_t right);
int64_t insert_into_leaf (int64_t leaf, int64_t key, char * value);

int64_t insert_into_internal_after_splitting(int64_t root, int64_t old_internal, int left_index, int64_t key, int64_t right);
int64_t insert_into_leaf_after_splitting(int64_t root, int64_t leaf, int64_t key, char * value);
int insert (int64_t key, char * value);


int64_t find_leaf( int64_t root, int64_t key);
char * find (int64_t key);

int64_t remove_entry_from_page( int64_t now, int64_t key);
int64_t adjust_root( int64_t root);
int64_t coalesce_nodes( int64_t root, int64_t now, int64_t neighbor, int neighbor_index, int64_t k_prime);
int64_t redistribute_nodes( int64_t root, int64_t now, int64_t neighbor, int neighbor_index, int k_prime_index, int64_t k_prime);
int64_t delete_entry(int64_t root, int64_t now, int64_t key);
int delete (int64_t key);

#endif
