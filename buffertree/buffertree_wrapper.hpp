#ifndef __buffertree_wrapper_HPP__
#define __buffertree_wrapper_HPP__

#include "tree_api.hpp"

#include <mutex>
#include <shared_mutex>
#include <string>
#include <vector>
#include <cstring>
#include <algorithm>
#include <thread>
#include <list>
#include <limits>

template <typename Key, typename T>
class buffertree_wrapper : public tree_api
{
public:
    buffertree_wrapper();
    virtual ~buffertree_wrapper();

    virtual bool find(const char *key, size_t key_sz, char *value_out) override;
    virtual bool insert(const char *key, size_t key_sz, const char *value, size_t value_sz) override;
    virtual bool update(const char *key, size_t key_sz, const char *value, size_t value_sz) override;
    virtual bool remove(const char *key, size_t key_sz) override;
    virtual int scan(const char *key, size_t key_sz, int scan_sz, char *&values_out) override;

    struct btree_node
    {
        Key key[4096 / 2 / (sizeof(Key) + sizeof(uint32_t)) - 1];
        uint32_t nxt[4096 / 2 / (sizeof(Key) + sizeof(uint32_t)) - 1];
        uint32_t num_item;
        uint32_t num_buf;
        Key buf_key[4096 / 2 / (sizeof(Key) + sizeof(T)) - 1];
        T buf_val[4096 / 2 / (sizeof(Key) + sizeof(T)) - 1];
        char padding[8];
    };
    struct btree_data
    {
        Key key[4096 / (sizeof(Key) + sizeof(T)) - 1];
        T val[4096 / (sizeof(Key) + sizeof(T)) - 1];
        uint32_t num_item;
        char padding[4];
    };
    void spill(uint32_t cur_id, btree_node *pnode);

private:
    // key: 1   50  100     200     x
    // nxt: <1  <50 <100    <200    >=200
    // num_item at least 2 for a btree_node

    std::shared_mutex mutex_;
    uint32_t node_cap = 4096 / 2 / (sizeof(Key) + sizeof(uint32_t)) - 1;
    uint32_t node_buf_cap = 4096 / 2 / (sizeof(Key) + sizeof(uint32_t)) - 1;
    uint32_t data_cap = 4096 / (sizeof(Key) + sizeof(T)) - 1;
    std::vector<FILE *> nodes;
    std::vector<bool> is_leaf;
    std::shared_mutex print_mutex, print_small_mutex;
    std::shared_mutex new_mutex;
    uint32_t root_id;

    btree_node *get_node(uint32_t id)
    {
        btree_node *node = new btree_node;
        size_t state;
        rewind(nodes[id]);
        state = fread(node, sizeof(btree_node), 1, nodes[id]);
        if (state != 1)
        {
            fprintf(stderr, "btree: I/O error in get_node\n");
            abort();
        }

        return node;
    }

    void set_node(uint32_t id, btree_node *node)
    {
        size_t state;
        rewind(nodes[id]);
        state = fwrite(node, sizeof(btree_node), 1, nodes[id]);
        delete node;

        if (state != 1)
        {
            fprintf(stderr, "btree: I/O error in set_node\n");
            abort();
        }
    }

    uint32_t init_new_node(btree_node *node = nullptr)
    {
        std::unique_lock lock(new_mutex);
        auto id = nodes.size();
        // printf("adding new node %lld\n", id);
        std::string file_name = "./btree/btree_node_" + std::to_string(id);
        nodes.push_back(fopen(file_name.c_str(), "w+b"));
        is_leaf.push_back(false);
        if (node == nullptr)
        {
            node = new btree_node;
            memset(node, 0, sizeof(btree_node));
            set_node(id, node);
        }
        else
        {
            set_node(id, node);
        }
        return id;
    }

    btree_data *get_data(uint32_t id)
    {

        btree_data *data = new btree_data;
        size_t state;
        rewind(nodes[id]);
        state = fread(data, sizeof(btree_data), 1, nodes[id]);
        if (state != 1)
        {
            fprintf(stderr, "btree: I/O error in get_data\n");
            abort();
        }
        return data;
    }

    void set_data(uint32_t id, btree_data *data)
    {
        size_t state;
        rewind(nodes[id]);
        state = fwrite(data, sizeof(btree_data), 1, nodes[id]);

        delete data;

        if (state != 1)
        {
            fprintf(stderr, "btree: I/O error in set_data\n");
            abort();
        }
    }

    uint32_t init_new_data(btree_data *data = nullptr)
    {
        std::unique_lock lock(new_mutex);
        auto id = nodes.size();
        // printf("adding new data %lld\n", id);
        std::string file_name = "./btree/btree_data_" + std::to_string(id);
        nodes.push_back(fopen(file_name.c_str(), "w+b"));
        is_leaf.push_back(true);
        if (data == nullptr)
        {
            data = new btree_data;
            memset(data, 0, sizeof(btree_data));
            set_data(id, data);
        }
        else
        {
            set_data(id, data);
        }
        return id;
    }

    uint32_t get_nxt_id(btree_node *node, Key key)
    {
        auto loc = std::upper_bound(node->key, node->key + node->num_item - 1, key);
        // printf("=%lld get %lld\n", key, loc - node->key);
        return node->nxt[loc - node->key];
    }

    bool get_buf_val(btree_node *node, Key key, T &val)
    {
        for (size_t i = 0; i < node->num_buf; ++i)
        {
            if (node->buf_key[i] == key)
            {
                val = node->buf_val[i];
                return true;
            }
        }
        return false;
    }

    bool get_nxt_val(btree_data *data, Key key, T &val)
    {
        auto loc = std::lower_bound(data->key, data->key + data->num_item, key);
        if (loc == data->key + data->num_item || *loc != key)
        {
            return false;
        }
        val = data->val[loc - data->key];
        return true;
    }

    bool set_nxt_val(btree_data *data, Key key, T &val)
    {
        auto loc = std::lower_bound(data->key, data->key + data->num_item, key);
        if (loc == data->key + data->num_item || *loc != key)
        {
            return false;
        }
        data->val[loc - data->key] = val;
        return true;
    }

    bool del_nxt_val(btree_data *data, Key key)
    {
        auto loc = std::lower_bound(data->key, data->key + data->num_item, key);
        if (loc == data->key + data->num_item || *loc != key)
        {
            return false;
        }
        size_t diff = loc - data->key;
        --data->num_item;
        memmove(data->key + diff, data->key + diff + 1, (data->num_item - diff) * sizeof(Key));
        memmove(data->val + diff, data->val + diff + 1, (data->num_item - diff) * sizeof(T));
        return true;
    }

    void print_data(btree_data *data, size_t num)
    {
        std::unique_lock print_lock(print_small_mutex);
        printf("key: ");
        for (size_t i = 0; i < num; ++i)
        {
            printf("%12lld", data->key[i]);
        }
        printf("\nval: ");
        for (size_t i = 0; i < num; ++i)
        {
            printf("%12lld", data->val[i]);
        }
        printf("\n");
    }

    void print_node(btree_node *node, size_t num)
    {
        std::unique_lock print_lock(print_small_mutex);
        printf("key: ");
        for (size_t i = 0; i < num - 1; ++i)
        {
            printf("%12lld", node->key[i]);
        }
        printf("\nnxt: ");
        for (size_t i = 0; i < num; ++i)
        {
            printf("%12lld", node->nxt[i]);
        }
        printf("\n");
    }

    void insert_data_item(btree_data *data, Key key, T &val)
    {
        size_t i = std::upper_bound(data->key, data->key + data->num_item, key) - data->key;
        /*for (i = 0; i < data->num_item; ++i)
        {
            if (key <= data->key[i])
            {
                break;
            }
        }*/
        if (i < data->num_item)
        {
            memmove(data->key + i + 1, data->key + i, (data->num_item - i) * sizeof(Key));
            memmove(data->val + i + 1, data->val + i, (data->num_item - i) * sizeof(T));
        }
        data->key[i] = key;
        data->val[i] = val;
        ++data->num_item;
        // printf("==%lld %lld %lld\n", i, key, val);
        // print_data(data);
    }
    // old:  l   r
    //       ld  rd
    // new:  l   key r
    // new:  ld  nxt  rd
    void insert_node_item(btree_node *node, Key key, uint32_t nxt)
    {
        size_t i = std::upper_bound(node->key, node->key + node->num_item - 1, key) - node->key;
        /*for (i = 0; i + 1 < node->num_item; ++i)
        {
            if (key <= node->key[i])
            {
                break;
            }
        }*/
        if (i < node->num_item)
        {
            memmove(node->key + i + 1, node->key + i, (node->num_item - 1 - i) * sizeof(Key));
            memmove(node->nxt + i + 1, node->nxt + i, (node->num_item - i) * sizeof(uint32_t));
        }
        node->key[i] = key;
        node->nxt[i] = nxt;
        ++node->num_item;
        // printf("==%lld %lld %lld\n", i, key, nxt);
        // print_node(node);
    }

    void print_state(size_t id, bool iterative = false)
    {
        if (!is_leaf[id])
        {
            btree_node *node = get_node(id);
            {
                std::unique_lock print_lock(print_small_mutex);
                printf("node %lld, num item %lld, num buf %lld:\n", id, node->num_item, node->num_buf);
                printf("key: ");
                for (size_t i = 0; i < node->num_item - 1; ++i)
                {
                    printf("%12lld", node->key[i]);
                }
                printf("\nnxt: ");
                for (size_t i = 0; i < node->num_item; ++i)
                {
                    printf("%12lld", node->nxt[i]);
                }
                printf("\nbufkey");
                for (size_t i = 0; i < node->num_buf; ++i)
                {
                    printf("%12lld", node->buf_key[i]);
                }
                printf("\nbufval: ");
                for (size_t i = 0; i < node->num_buf; ++i)
                {
                    printf("%12lld", node->buf_val[i]);
                }
                printf("\n");
            }
            if (iterative)
            {
                for (size_t i = 0; i < node->num_item; ++i)
                {
                    print_state(node->nxt[i], true);
                }
            }
        }
        else
        {
            btree_data *data = get_data(id);
            {
                std::unique_lock print_lock(print_small_mutex);
                printf("data %lld:\n", id);
                printf("key: ");
                for (size_t i = 0; i < data->num_item; ++i)
                {
                    printf("%12lld", data->key[i]);
                }
                printf("\nval: ");
                for (size_t i = 0; i < data->num_item; ++i)
                {
                    printf("%12lld", data->val[i]);
                }
                printf("\n");
            }
        }
    }

    // must add lock before call
    void split_data(btree_data *data_r, btree_node *node_fa)
    {
        btree_data *data_l = new btree_data;
        memcpy(data_l->key, data_r->key, data_cap / 2 * sizeof(Key));
        memcpy(data_l->val, data_r->val, data_cap / 2 * sizeof(T));
        data_l->num_item = data_cap / 2;
        uint32_t nxt = init_new_data(data_l);
        uint32_t k = data_r->key[data_cap / 2];
        memmove(data_r->key, data_r->key + data_cap / 2, (data_cap - data_cap / 2) * sizeof(Key));
        memmove(data_r->val, data_r->val + data_cap / 2, (data_cap - data_cap / 2) * sizeof(T));
        data_r->num_item = data_cap - data_cap / 2;
        insert_node_item(node_fa, k, nxt);
    }

    // must add lock before call
    void split_node(btree_node *node_r, btree_node *node_fa)
    {
        btree_node *node_l = new btree_node;
        memcpy(node_l->key, node_r->key, (node_cap / 2 - 1) * sizeof(Key));
        memcpy(node_l->nxt, node_r->nxt, node_cap / 2 * sizeof(uint32_t));
        node_l->num_item = node_cap / 2;
        uint32_t k = node_r->key[node_cap / 2 - 1];

        uint32_t num_buf = node_r->num_buf;
        node_l->num_buf = 0;
        node_r->num_buf = 0;
        for (size_t i = 0; i < num_buf; ++i)
        {
            if (node_r->buf_key[i] >= k)
            {
                node_r->buf_key[node_r->num_buf] = node_r->buf_key[i];
                node_r->buf_val[node_r->num_buf] = node_r->buf_val[i];
                ++node_r->num_buf;
            }
            else
            {
                node_l->buf_key[node_l->num_buf] = node_r->buf_key[i];
                node_l->buf_val[node_l->num_buf] = node_r->buf_val[i];
                ++node_l->num_buf;
            }
        }

        uint32_t nxt = init_new_node(node_l);
        memmove(node_r->key, node_r->key + node_cap / 2, (node_cap - node_cap / 2 - 1) * sizeof(Key));
        memmove(node_r->nxt, node_r->nxt + node_cap / 2, (node_cap - node_cap / 2) * sizeof(uint32_t));
        node_r->num_item = node_cap - node_cap / 2;
        insert_node_item(node_fa, k, nxt);
    }
};

template <typename Key, typename T>
buffertree_wrapper<Key, T>::buffertree_wrapper()
{
    btree_node *node = new btree_node;
    node->nxt[0] = 1;
    node->key[0] = 2e9;
    node->nxt[1] = 2;
    node->num_item = 2;
    node->num_buf = 0;
    init_new_node(node);
    init_new_data();
    init_new_data();
    root_id = 0;
}

template <typename Key, typename T>
buffertree_wrapper<Key, T>::~buffertree_wrapper()
{
    for (auto node : nodes)
    {
        fclose(node);
    }
}

template <typename Key, typename T>
bool buffertree_wrapper<Key, T>::find(const char *key, size_t key_sz, char *value_out)
{

    Key k = *reinterpret_cast<Key *>(const_cast<char *>(key));
    // printf("find %lld\n", k);
    int cur_id = root_id;
    T v;
    while (!is_leaf[cur_id])
    {
        btree_node *node = get_node(cur_id);
        if (get_buf_val(node, k, v))
        {
            memcpy(value_out, &v, sizeof(T));
            // printf("find end succ\n");
            return true;
        }
        int pre_id = cur_id;
        cur_id = get_nxt_id(node, k);
        delete node;
    }
    btree_data *data = get_data(cur_id);
    bool succ;
    succ = get_nxt_val(data, k, v);
    delete data;
    if (!succ)
    {
        // printf("find end fail\n");
        return false;
    }
    memcpy(value_out, &v, sizeof(T));
    // printf("find end succ\n");
    return true;
}

template <typename Key, typename T>
bool buffertree_wrapper<Key, T>::insert(const char *key, size_t key_sz, const char *value, size_t value_sz)
{
    Key k = *reinterpret_cast<Key *>(const_cast<char *>(key));
    T v = *reinterpret_cast<T *>(const_cast<char *>(value));
    // printf("=======insert========%12lld %12lld\n", k, v);
    //  print_state(root_id, true);
    uint32_t cur_id = root_id;
    btree_node *node = get_node(cur_id);
    node->buf_key[node->num_buf] = k;
    node->buf_val[node->num_buf] = v;
    ++node->num_buf;
    // printf("0\n");
    if (node->num_buf == node_buf_cap)
    {
        // printf("1\n");
        spill(root_id, node);
        // printf("2\n");
        if (node->num_item == node_cap)
        { // change root
            // printf("3\n");
            btree_node *root_node = new btree_node;
            root_node->nxt[0] = root_id;
            root_node->num_item = 1;
            root_node->num_buf = 0;
            split_node(node, root_node);
            root_id = init_new_node(root_node);
        }
    }
    // printf("4\n");
    set_node(cur_id, node);
    // if (cur_id != root_id)
    //  print_state(root_id, true);
    return true;
}

// clear node[cur_id]'s buffer
template <typename Key, typename T>
void buffertree_wrapper<Key, T>::spill(uint32_t cur_id, btree_node *pnode)
{
    while (pnode->num_buf > 0)
    {
        Key k = pnode->buf_key[pnode->num_buf - 1];
        T v = pnode->buf_val[pnode->num_buf - 1];
        --pnode->num_buf;
        uint32_t nxt_id = get_nxt_id(pnode, k);
        if (is_leaf[nxt_id])
        {
            btree_data *data = get_data(nxt_id);
            insert_data_item(data, k, v);
            if (data->num_item == data_cap)
            {
                split_data(data, pnode);
            }
            set_data(nxt_id, data);
        }
        else
        {
            btree_node *node = get_node(nxt_id);
            node->buf_key[node->num_buf] = k;
            node->buf_val[node->num_buf] = v;
            ++node->num_buf;
            if (node->num_buf == node_buf_cap)
            {
                spill(root_id, node);
            }
            if (node->num_item == node_cap)
            {
                split_node(node, pnode);
            }
            set_node(nxt_id, node);
        }
        if (pnode->num_item == node_cap)
        { // pnode full, split buffer by parent function
            break;
        }
    }
}

template <typename Key, typename T>
bool buffertree_wrapper<Key, T>::update(const char *key, size_t key_sz, const char *value, size_t value_sz)
{
    // printf("==update==\n");
    Key k = *reinterpret_cast<Key *>(const_cast<char *>(key));
    T v = *reinterpret_cast<T *>(const_cast<char *>(value));
    int cur_id = root_id;

    while (!is_leaf[cur_id])
    {
        btree_node *node = get_node(cur_id);
        // printf("0 %lld %lld/%lld\n", cur_id, node->num_item, node_cap);
        int pre_id = cur_id;
        cur_id = get_nxt_id(node, k);
        delete node;
    }
    btree_data *data = get_data(cur_id);
    bool succ;
    succ = set_nxt_val(data, k, v);
    set_data(cur_id, data);
    return true;
}

template <typename Key, typename T>
bool buffertree_wrapper<Key, T>::remove(const char *key, size_t key_sz)
{
    // printf("==remove==\n");
    Key k = *reinterpret_cast<Key *>(const_cast<char *>(key));
    int cur_id = root_id;
    while (!is_leaf[cur_id])
    {
        btree_node *node = get_node(cur_id);
        // printf("0 %lld %lld/%lld\n", cur_id, node->num_item, node_cap);
        int pre_id = cur_id;
        cur_id = get_nxt_id(node, k);
        delete node;
    }
    btree_data *data = get_data(cur_id);
    bool succ;
    succ = del_nxt_val(data, k);
    set_data(cur_id, data);
    return true;
}

template <typename Key, typename T>
int buffertree_wrapper<Key, T>::scan(const char *key, size_t key_sz, int scan_sz, char *&values_out)
{
    std::shared_lock lock(mutex_);
    return scan_sz;
}

#endif