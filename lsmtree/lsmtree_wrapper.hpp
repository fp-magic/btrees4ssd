#ifndef __LSMTREE_WRAPPER_HPP__
#define __LSMTREE_WRAPPER_HPP__

#include "tree_api.hpp"
#include <string>
#include <vector>
#include <cassert>
#include <sstream>
#include <cstdint>
#include <type_traits>
#include <cstring>
#include <array>
#include <mutex>
#include <shared_mutex>
#include <iostream>

#include "lsm_tree.h"
#include "sys.h"


#define KEY_MAX 2147483647

#define KEY_MIN -2147483648


class lsmtree_wrapper : public tree_api
{
public:
    lsmtree_wrapper();

    virtual  ~lsmtree_wrapper();
    
    virtual bool find(const char* key, size_t key_sz, char* value_out) override;
    virtual bool insert(const char* key, size_t key_sz, const char* value, size_t value_sz) override;
    virtual bool update(const char* key, size_t key_sz, const char* value, size_t value_sz) override;
    virtual bool remove(const char* key, size_t key_sz) override;
    virtual int scan(const char* key, size_t key_sz, int scan_sz, char*& values_out) override;

    //void print_stat(leveldb::DB* db_, bool print_sst=false);

private:
    LSMTree* lsm;
};

lsmtree_wrapper::lsmtree_wrapper() {
    int buffer_num_pages, buffer_max_entries, depth, fanout, num_threads;
    float bf_bits_per_entry;

    buffer_num_pages = 1000;
    depth = 5;
    fanout = 5;
    num_threads = 4;
    bf_bits_per_entry = 0.5;

    buffer_max_entries = buffer_num_pages * getpagesize() / sizeof(entry_t);

    lsm = new LSMTree(buffer_max_entries,depth,fanout,num_threads,bf_bits_per_entry);

}

lsmtree_wrapper::~lsmtree_wrapper(){
    // TODO(jhpark) : pass parameters for leveldb statistics
    //print_stat(db);
    //delete lsm;
    delete lsm;
}

bool lsmtree_wrapper::find(const char* key, size_t key_sz, char* value_out)
{
    // TODO(jhpark): Provide positive/false read statistics
    //std::string str;
    //uint64_t k = __builtin_bswap64(*reinterpret_cast<const uint64_t *>(key));
    int32_t k = __builtin_bswap32(*reinterpret_cast<const int32_t *>(key));

    //if (lsm->get(k)) return true;
    lsm->get(k);
    return true;

    //return false;
}


bool lsmtree_wrapper::insert(const char* key, size_t key_sz, const char* value, size_t value_sz)
{
    //uint64_t k = __builtin_bswap64(*reinterpret_cast<const uint64_t *>(key));
    //uint64_t v = __builtin_bswap64(*reinterpret_cast<const uint64_t *>(value));
    int32_t k = __builtin_bswap32(*reinterpret_cast<const int32_t *>(key));
    int32_t v = __builtin_bswap32(*reinterpret_cast<const int32_t *>(value));
    if (v < VAL_MIN || v > VAL_MAX) {
        die("Could not insert value " + to_string(v) + ": out of range.");
        return false;
    } else {
        lsm->put(k, v);
        return true;
    }

}

bool lsmtree_wrapper::update(const char* key, size_t key_sz, const char* value, size_t value_sz) {
  return insert(key, key_sz, value, value_sz);
}

bool lsmtree_wrapper::remove(const char* key, size_t key_sz) {

    //uint64_t k = __builtin_bswap64(*reinterpret_cast<const uint64_t *>(key));
    int32_t k = __builtin_bswap32(*reinterpret_cast<const int32_t *>(key));

    lsm->del(k);

    return true;

}

int lsmtree_wrapper::scan(const char* key, size_t key_sz, int scan_sz, char*& values_out) {
    // TODO(jhpark): 
    //  - Provide positive/false scan statistics
    //  - Efficient scan algorithms needed

    //uint64_t k = __builtin_bswap64(*reinterpret_cast<const uint64_t *>(key));
    int32_t k = __builtin_bswap32(*reinterpret_cast<const int32_t *>(key));

    lsm->range(k, k+scan_sz);

    return 1;
}

#endif