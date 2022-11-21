#ifndef __BTREE_WRAPPER_HPP__
#define __BTREE_WRAPPER_HPP__

#include "tree_api.hpp"

#include <mutex>
#include <shared_mutex>
#include <string>
#include <vector>

template <typename Key, typename T>
class btree_wrapper : public tree_api
{
public:
    btree_wrapper();
    virtual ~btree_wrapper();

    virtual bool find(const char *key, size_t key_sz, char *value_out) override;
    virtual bool insert(const char *key, size_t key_sz, const char *value, size_t value_sz) override;
    virtual bool update(const char *key, size_t key_sz, const char *value, size_t value_sz) override;
    virtual bool remove(const char *key, size_t key_sz) override;
    virtual int scan(const char *key, size_t key_sz, int scan_sz, char *&values_out) override;

private:
    std::shared_mutex mutex_;
    int node_size = 31;
    std::vector<FILE *> nodes;
    FILE *data;
};

template <typename Key, typename T>
btree_wrapper<Key, T>::btree_wrapper()
{
    nodes.clear();
    nodes.push_back(fopen("btree_node_0", "a+"));
    data = fopen("btree_data", "w");
}

template <typename Key, typename T>
btree_wrapper<Key, T>::~btree_wrapper()
{
    for (auto node : nodes)
    {
        fclose(node);
    }
    fclose(data);
}

template <typename Key, typename T>
bool btree_wrapper<Key, T>::find(const char *key, size_t key_sz, char *value_out)
{
    std::shared_lock lock(mutex_);

    Key k = *reinterpret_cast<Key *>(const_cast<char *>(key));
    

    return true;
}

template <typename Key, typename T>
bool btree_wrapper<Key, T>::insert(const char *key, size_t key_sz, const char *value, size_t value_sz)
{
    std::unique_lock lock(mutex_);
    return true;
}

template <typename Key, typename T>
bool btree_wrapper<Key, T>::update(const char *key, size_t key_sz, const char *value, size_t value_sz)
{
    std::unique_lock lock(mutex_);
    return true;
}

template <typename Key, typename T>
bool btree_wrapper<Key, T>::remove(const char *key, size_t key_sz)
{
    std::unique_lock lock(mutex_);
    return true;
}

template <typename Key, typename T>
int btree_wrapper<Key, T>::scan(const char *key, size_t key_sz, int scan_sz, char *&values_out)
{
    std::shared_lock lock(mutex_);
    return scan_sz;
}

#endif