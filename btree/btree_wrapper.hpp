#ifndef __BTREE_WRAPPER_HPP__
#define __BTREE_WRAPPER_HPP__

#include "tree_api.hpp"

#include <mutex>
#include <shared_mutex>
#include <string>
#include <vector>
#include <cstring>
#include <algorithm>

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
    // key: 1   50  100     200     x
    // nxt: <1  <50 <100    <200    >=200
    // num_item at least 2 for a btree_node
    struct btree_node
    {
        Key key[4096 / (sizeof(Key) + sizeof(size_t))];
        size_t nxt[4096 / (sizeof(Key) + sizeof(size_t))];
    };
    struct btree_data
    {
        Key key[4096 / (sizeof(Key) + sizeof(T))];
        T val[4096 / (sizeof(Key) + sizeof(T))];
    };

    std::shared_mutex mutex_;
    int node_cap = 4096 / (sizeof(Key) + sizeof(size_t));
    int data_cap = 4096 / (sizeof(Key) + sizeof(T));
    std::vector<FILE *> nodes;
    std::vector<bool> is_leaf;
    std::vector<size_t> num_item;

    btree_node *get_node(int id)
    {
        rewind(nodes[id]);
        btree_node *node = new btree_node;
        if (fread(node, sizeof(btree_node), 1, nodes[id]) != 1)
        {
            fprintf(stderr, "btree: I/O error in get_node\n");
            abort();
        }
        return node;
    }

    void set_node(int id, btree_node *node)
    {
        if (fwrite(node, sizeof(btree_node), 1, nodes[id]) != 1)
        {
            fprintf(stderr, "btree: I/O error in set_node\n");
            abort();
        }
    }

    void init_new_node(btree_node *node = nullptr, int num = 0)
    {
        auto id = nodes.size();
        std::string file_name = "btree_node_" + std::to_string(id);
        nodes.push_back(fopen(file_name.c_str(), "a+b"));
        is_leaf.push_back(false);
        num_item.push_back(num);
        if (node == nullptr)
        {
            node = new btree_node;
            memset(node, 0, sizeof(btree_node));
            set_node(id, node);
            delete node;
        }
        else
        {
            set_node(id, node);
        }
    }

    btree_data *get_data(int id)
    {
        rewind(nodes[id]);
        btree_data *data = new btree_data;
        if (fread(data, sizeof(btree_data), 1, nodes[id]) != 1)
        {
            fprintf(stderr, "btree: I/O error in get_data\n");
            abort();
        }
        return data;
    }

    void set_data(int id, btree_data *data)
    {
        if (fwrite(data, sizeof(btree_data), 1, nodes[id]) != 1)
        {
            fprintf(stderr, "btree: I/O error in set_data\n");
            abort();
        }
    }

    void init_new_data(btree_data *data = nullptr, int num = 0)
    {
        auto id = nodes.size();
        std::string file_name = "btree_data_" + std::to_string(id);
        nodes.push_back(fopen(file_name.c_str(), "a+b"));
        is_leaf.push_back(true);
        num_item.push_back(num);
        if (data == nullptr)
        {
            data = new btree_data;
            memset(data, 0, sizeof(btree_data));
            set_data(id, data);
            delete data;
        }
        else
        {
            set_data(id, data);
        }
    }

    size_t get_nxt_id(btree_node *node, Key key, size_t id)
    {
        auto loc = std::upper_bound(node->key, node->key + num_item[id] - 1, id);
        return node->nxt[loc - node->key];
    }

    void insert_data_item(btree_data *data, Key key, T &val, size_t id)
    {
        size_t i;
        for (i = 0; i < num_item[id]; ++i)
        {
            if (key >= data->key[i])
            {
                break;
            }
        }
        if (i < num_item[id])
        {
            memmove(data->key + i + 1, data->key + i, (num_item[id] - i) * sizeof(Key));
            memmove(data->val + i + 1, data->key + i, (num_item[id] - i) * sizeof(T));
        }
        data->key[i] = key;
        data->val[i] = val;
        ++num_item[id];
    }
    // old:  l   r
    //       ld  rd
    // new:  l   key r
    // new:  ld  nxt  rd
    void insert_node_item(btree_node *node, Key key, uint64_t nxt, size_t id)
    {
        size_t i;
        for (i = 0; i < num_item[id]; ++i)
        {
            if (key >= node->key[i])
            {
                break;
            }
        }
        if (i < num_item[id])
        {
            memmove(node->key + i + 1, node->key + i, (num_item[id] - i) * sizeof(Key));
            memmove(node->nxt + i + 1, node->key + i, (num_item[id] - i) * sizeof(uint64_t));
        }
        node->key[i] = key;
        node->nxt[i] = nxt;
        ++num_item[id];
    }
};

template <typename Key, typename T>
btree_wrapper<Key, T>::btree_wrapper()
{
    btree_node *node = new btree_node;
    node->nxt[0] = 1;
    node->key[0] = 0;
    node->nxt[1] = 2;
    init_new_node(node, 2);
    delete node;
    init_new_data();
    init_new_data();
}

template <typename Key, typename T>
btree_wrapper<Key, T>::~btree_wrapper()
{
    for (auto node : nodes)
    {
        fclose(node);
    }
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
    Key k = *reinterpret_cast<Key *>(const_cast<char *>(key));
    T v = *reinterpret_cast<T *>(const_cast<char *>(value));
    int cur_id = 0;

    // find leaf node
    while (!is_leaf[cur_id])
    {
        btree_node *node = get_node(cur_id);
        get_nxt_id(node, k, cur_id);
        delete node;
    }

    // check cap
    if (num_item[cur_id] < data_cap - 1)
    {
        btree_data *data = get_data(cur_id);
        insert_data_item(data, k, v, cur_id);
        set_data(cur_id, data);
        delete data;
    }
    else
    {
        // full, need split
    }
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