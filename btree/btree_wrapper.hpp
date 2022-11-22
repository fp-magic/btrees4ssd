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
    bool insert(const char *key, size_t key_sz, const char *value, size_t value_sz, size_t lock_mode);

private:
    // key: 1   50  100     200     x
    // nxt: <1  <50 <100    <200    >=200
    // num_item at least 2 for a btree_node

    struct btree_node
    {
        Key key[64 / (sizeof(Key) + sizeof(size_t)) - 1];
        size_t nxt[64 / (sizeof(Key) + sizeof(size_t)) - 1];
        size_t num_item;
    };
    struct btree_data
    {
        Key key[64 / (sizeof(Key) + sizeof(T)) - 1];
        T val[64 / (sizeof(Key) + sizeof(T)) - 1];
        size_t num_item;
    };

    std::shared_mutex mutex_;
    int node_cap = 64 / (sizeof(Key) + sizeof(size_t)) - 1;
    int data_cap = 64 / (sizeof(Key) + sizeof(T)) - 1;
    std::vector<FILE *> nodes;
    std::vector<bool> is_leaf;
    std::vector<std::shared_mutex *> small_mutex;
    std::vector<std::shared_mutex *> large_mutex;
    std::shared_mutex root_mutex, print_mutex, print_small_mutex;
    std::shared_mutex new_mutex;
    int root_id;

    btree_node *get_node(int id)
    {
        btree_node *node = new btree_node;
        size_t state;
        {
            std::unique_lock lock(*small_mutex[id]);
            rewind(nodes[id]);
            state = fread(node, sizeof(btree_node), 1, nodes[id]);
        }
        if (state != 1)
        {
            fprintf(stderr, "btree: I/O error in get_node\n");
            abort();
        }

        return node;
    }

    void set_node(int id, btree_node *node)
    {
        size_t state;
        {
            std::unique_lock lock(*small_mutex[id]);
            rewind(nodes[id]);
            state = fwrite(node, sizeof(btree_node), 1, nodes[id]);
        }
        if (state != 1)
        {
            fprintf(stderr, "btree: I/O error in set_node\n");
            abort();
        }
    }

    size_t init_new_node(btree_node *node = nullptr)
    {
        std::unique_lock lock(new_mutex);
        auto id = nodes.size();
        // printf("adding new node %lld\n", id);
        std::string file_name = "./btree/btree_node_" + std::to_string(id);
        nodes.push_back(fopen(file_name.c_str(), "w+b"));
        is_leaf.push_back(false);
        small_mutex.push_back(new std::shared_mutex);
        large_mutex.push_back(new std::shared_mutex);
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
        return id;
    }

    btree_data *get_data(int id)
    {

        btree_data *data = new btree_data;
        size_t state;
        {
            std::unique_lock lock(*small_mutex[id]);
            rewind(nodes[id]);
            state = fread(data, sizeof(btree_data), 1, nodes[id]);
        }
        if (state != 1)
        {
            fprintf(stderr, "btree: I/O error in get_data\n");
            abort();
        }
        return data;
    }

    void set_data(int id, btree_data *data)
    {
        size_t state;
        {
            std::unique_lock lock(*small_mutex[id]);
            rewind(nodes[id]);
            state = fwrite(data, sizeof(btree_data), 1, nodes[id]);
        }
        if (state != 1)
        {
            fprintf(stderr, "btree: I/O error in set_data\n");
            abort();
        }
    }

    size_t init_new_data(btree_data *data = nullptr)
    {
        std::unique_lock lock(new_mutex);
        auto id = nodes.size();
        // printf("adding new data %lld\n", id);
        std::string file_name = "./btree/btree_data_" + std::to_string(id);
        nodes.push_back(fopen(file_name.c_str(), "w+b"));
        is_leaf.push_back(true);
        small_mutex.push_back(new std::shared_mutex);
        large_mutex.push_back(new std::shared_mutex);
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
        return id;
    }

    size_t get_nxt_id(btree_node *node, Key key)
    {
        auto loc = std::upper_bound(node->key, node->key + node->num_item - 1, key);
        // printf("=%lld get %lld\n", key, loc - node->key);
        return node->nxt[loc - node->key];
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
        size_t i;
        // print_data(data);
        for (i = 0; i < data->num_item; ++i)
        {
            if (key <= data->key[i])
            {
                break;
            }
        }
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
    void insert_node_item(btree_node *node, Key key, size_t nxt)
    {
        size_t i;
        // print_node(node);
        for (i = 0; i + 1 < node->num_item; ++i)
        {
            if (key <= node->key[i])
            {
                break;
            }
        }
        if (i < node->num_item)
        {
            memmove(node->key + i + 1, node->key + i, (node->num_item - 1 - i) * sizeof(Key));
            memmove(node->nxt + i + 1, node->nxt + i, (node->num_item - i) * sizeof(size_t));
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
                printf("node %lld:\n", id);
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
        size_t nxt = init_new_data(data_l);
        delete data_l;
        size_t k = data_r->key[data_cap / 2];
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
        memcpy(node_l->nxt, node_r->nxt, node_cap / 2 * sizeof(size_t));
        node_l->num_item = node_cap / 2;
        size_t nxt = init_new_node(node_l);
        delete node_l;
        size_t k = node_r->key[node_cap / 2 - 1];
        memmove(node_r->key, node_r->key + node_cap / 2, (node_cap - node_cap / 2 - 1) * sizeof(Key));
        memmove(node_r->nxt, node_r->nxt + node_cap / 2, (node_cap - node_cap / 2) * sizeof(size_t));
        node_r->num_item = node_cap - node_cap / 2;
        insert_node_item(node_fa, k, nxt);
    }

    void down_insert_lock(std::vector<size_t> &x_locked, size_t lock_mode, size_t cur_id, size_t pre_id = -1)
    {
        if (pre_id == -1)
        { // the first down lock
            if (lock_mode == 0)
            {
                // printf("00before slock %lld\n", cur_id);
                large_mutex[cur_id]->lock_shared();
                // printf("01slock %lld\n", cur_id);
            }
            else
            {
                // printf("%lld prelock %lld\n", k_b, cur_id);
                large_mutex[cur_id]->lock();
                x_locked.push_back(cur_id);
                // printf("%lld lock %lld\n", k_b, cur_id);
            }
        }
        else
        {
            if (lock_mode == 0)
            {
                if (!is_leaf[cur_id])
                {
                    // printf("11before slock %lld\n", cur_id);
                    large_mutex[cur_id]->lock_shared();
                    // printf("12slock %lld\n", cur_id);
                    // printf("13before un slock %lld\n", pre_id);
                    large_mutex[pre_id]->unlock_shared();
                    // printf("14un slock %lld\n", pre_id);
                }
                else
                {
                    // printf("21before xlock %lld\n", cur_id);
                    large_mutex[cur_id]->lock();
                    // printf("22xlock %lld\n", cur_id);
                    x_locked.push_back(cur_id);
                    // printf("23before un slock %lld\n", pre_id);
                    large_mutex[pre_id]->unlock_shared();
                    // printf("24un slock %lld\n", pre_id);
                }
            }
            else
            {
                // printf("%lld prelock %lld\n", k_b, cur_id);
                large_mutex[cur_id]->lock();
                // printf("%lld lock %lld\n", k_b, cur_id);
                x_locked.push_back(cur_id);
            }
        }
    }

    void revert_first_down_insert_lock(std::vector<size_t> &x_locked, size_t lock_mode, size_t cur_id)
    {
        if (lock_mode == 0)
        {
            // printf("00before slock %lld\n", cur_id);
            large_mutex[cur_id]->unlock_shared();
            // printf("01slock %lld\n", cur_id);
        }
        else
        {
            // printf("%lld prelock %lld\n", k_b, cur_id);
            large_mutex[cur_id]->unlock();
            x_locked.pop_back();
            // printf("%lld lock %lld\n", k_b, cur_id);
        }
    }

    bool up_insert_lock(std::vector<size_t> &x_locked, size_t lock_mode, size_t cur_id, size_t nxt_id = -1)
    {
        if (nxt_id == -1)
        { // cur_id is root
          // do nothing
        }
        else
        {
            if (lock_mode == 0)
            {
                if (!large_mutex[nxt_id]->try_lock())
                {
                    // printf("31before un xlock %lld\n", pre_id);
                    return false;
                    // printf("32un xlock %lld\n", pre_id);
                }
                else
                {
                    // printf("41xlock %lld\n", *id_it);
                    // printf("42before un xlock %lld\n", pre_id);
                    large_mutex[cur_id]->unlock();
                    // printf("43un xlock %lld\n", pre_id);
                    x_locked.pop_back();
                    x_locked.push_back(nxt_id);
                }
            }
            else
            {
                // do nothing
            }
        }
        return true;
    }
};

template <typename Key, typename T>
btree_wrapper<Key, T>::btree_wrapper()
{
    btree_node *node = new btree_node;
    node->nxt[0] = 1;
    node->key[0] = 0;
    node->nxt[1] = 2;
    node->num_item = 2;
    init_new_node(node);
    delete node;
    init_new_data();
    init_new_data();
    root_id = 0;
}

template <typename Key, typename T>
btree_wrapper<Key, T>::~btree_wrapper()
{
    for (auto node : nodes)
    {
        fclose(node);
    }
    for (auto m : small_mutex)
    {
        delete m;
    }
}

template <typename Key, typename T>
bool btree_wrapper<Key, T>::find(const char *key, size_t key_sz, char *value_out)
{
    // printf("find\n");
    Key k = *reinterpret_cast<Key *>(const_cast<char *>(key));
    int cur_id;
    {
        std::shared_lock lock(root_mutex);
        cur_id = root_id;
    }
    large_mutex[cur_id]->lock_shared();
    while (!is_leaf[cur_id])
    {
        btree_node *node = get_node(cur_id);
        // printf("0 %lld %lld/%lld\n", cur_id, node->num_item, node_cap);
        int pre_id = cur_id;
        cur_id = get_nxt_id(node, k);
        large_mutex[cur_id]->lock_shared();
        large_mutex[pre_id]->unlock_shared();
        delete node;
    }
    btree_data *data = get_data(cur_id);
    bool succ;
    T v;
    succ = get_nxt_val(data, k, v);
    large_mutex[cur_id]->unlock_shared();
    delete data;
    if (!succ)
    {

        // print_mutex.lock();
        {
            std::unique_lock print_lock(print_small_mutex);
            printf("find %lld failed\n", k);
        }
        // print_state(root_id, true);
        // print_mutex.unlock();
        return false;
    }
    memcpy(value_out, &v, sizeof(T));
    // printf("find end\n");
    return true;
}

template <typename Key, typename T>
bool btree_wrapper<Key, T>::insert(const char *key, size_t key_sz, const char *value, size_t value_sz)
{
    Key k = *reinterpret_cast<Key *>(const_cast<char *>(key));
    for (size_t i = 0; i < 10; ++i)
    {
        if (i == 0)
        {
            if (insert(key, key_sz, value, value_sz, 1))
            {
                printf("%lld succ in %lld time(s)\n", key, i + 1);
                return true;
            }
        }
        else
        {
            if (insert(key, key_sz, value, value_sz, 1))
            {
                printf("%lld succ in %lld time(s)\n", key, i + 1);
                return true;
            }
        }
    }
    printf("%lld fail", key);
    return false;
}

template <typename Key, typename T>
bool btree_wrapper<Key, T>::insert(const char *key, size_t key_sz, const char *value, size_t value_sz, size_t lock_mode)
{
    // printf("---------------insert-----------\n");
    Key k = *reinterpret_cast<Key *>(const_cast<char *>(key));
    Key k_b = k;
    T v = *reinterpret_cast<T *>(const_cast<char *>(value));
    std::vector<size_t> cur_ids;
    std::vector<std::pair<size_t, btree_data *>> uncommit_data;
    std::vector<std::pair<size_t, btree_node *>> uncommit_node;
    std::vector<size_t> x_locked;
    bool succ = true;
    bool root_changed = false;
    // printf("k%lld v %lld mode%lld\n", k, v, lock_mode);
    //   print_state(root_id, true);
    size_t cur_id;
    {
        std::shared_lock lock(root_mutex);
        cur_id = root_id;
    }
    down_insert_lock(x_locked, lock_mode, cur_id);
    /*{
        std::unique_lock print_lock(print_small_mutex);
        printf("%lld lock %lld\n", k_b, cur_id);
    }*/
    {
        std::shared_lock lock(root_mutex);
        if (cur_id != root_id)
        { // root changed
            revert_first_down_insert_lock(x_locked, lock_mode, cur_id);
            /*{
                std::unique_lock print_lock(print_small_mutex);
                printf("%lld unlock %lld\n", k_b, cur_id);
            }*/
            return false;
        }
    }
    //  find leaf node

    while (!is_leaf[cur_id])
    {
        btree_node *node = get_node(cur_id);
        cur_ids.push_back(cur_id);
        int pre_id = cur_id;
        cur_id = get_nxt_id(node, k);
        delete node;
        down_insert_lock(x_locked, lock_mode, cur_id, pre_id);
        /*{
            std::unique_lock print_lock(print_small_mutex);
            printf("%lld lock %lld\n", k_b, cur_id);
        }*/
    }

    // insert
    btree_data *data = get_data(cur_id);
    insert_data_item(data, k, v);
    /*{
        std::unique_lock print_lock(print_small_mutex);
        printf("%lld k%lld val %lld inserted to %lld\n", k_b, k, v, cur_id);
    }*/
    if (data->num_item < data_cap)
    {
        set_data(cur_id, data);
        delete data;
    }
    else
    {
        // printf("%lld 2\n",k);
        auto id_it = cur_ids.rbegin();
        btree_node *cur_node;
        do
        {
            if (up_insert_lock(x_locked, lock_mode, cur_id, *id_it))
            {
                btree_node *nxt_node = get_node(*id_it);
                if (id_it == cur_ids.rbegin())
                {
                    split_data(data, nxt_node);
                    /*{
                        std::unique_lock print_lock(print_small_mutex);
                        printf("%lld k%lld split to %lld\n", k_b, cur_id, *id_it);
                    }*/
                    uncommit_data.push_back({cur_id, data});
                    uncommit_node.push_back({*id_it, nxt_node});
                }
                else
                {
                    split_node(cur_node, nxt_node);
                    /*{
                        std::unique_lock print_lock(print_small_mutex);
                        printf("%lld k%lld split to %lld\n", k_b, cur_id, *id_it);
                    }*/
                    uncommit_node.push_back({*id_it, nxt_node});
                }
                cur_node = nxt_node;
                cur_id = *id_it;
            }
            else
            {
                succ = false;
                break;
            }
            ++id_it;
        } while (cur_node->num_item == node_cap && id_it != cur_ids.rend());
        if (cur_node->num_item == node_cap)
        { // root is full
            btree_node *nxt_node = new btree_node;
            nxt_node->nxt[0] = root_id;
            nxt_node->num_item = 1;
            split_node(cur_node, nxt_node);
            size_t new_root_id = init_new_node(nxt_node);
            // printf("%lld prelock %lld\n", k_b, new_root_id);
            large_mutex[new_root_id]->lock();
            // printf("%lld lock %lld\n", k_b, new_root_id);
            x_locked.push_back(new_root_id);
            root_mutex.lock();
            root_changed = true;
            /*{
                std::unique_lock print_lock(print_small_mutex);
                printf("%lld root id change from %lld to %lld\n", k_b, root_id, new_root_id);
            }*/
            root_id = new_root_id;
            delete nxt_node;
        }
    }
    if (succ)
    {
        for (auto id_data : uncommit_data)
        {
            // printf("%lld update %lld\n", k_b, id_data.first);
            set_data(id_data.first, id_data.second);
            delete id_data.second;
        }
        for (auto id_node : uncommit_node)
        {
            // printf("%lld update %lld\n", k_b, id_node.first);
            set_node(id_node.first, id_node.second);
            delete id_node.second;
        }
        for (auto it = x_locked.rbegin(); it != x_locked.rend(); ++it)
        {
            // printf("51before un xlock %lld\n", id);
            large_mutex[*it]->unlock();
            /*{
                std::unique_lock print_lock(print_small_mutex);
                printf("%lld unlock %lld\n", k_b, *it);
            }*/
            // printf("%lld unlock %lld\n", k_b, id);
            //   printf("52un xlock %lld\n", id);
        }
        if (root_changed)
        {
            root_mutex.unlock();
        }
    }
    /*print_mutex.lock();
    {
        std::unique_lock print_lock(print_small_mutex);
        printf("state of %lld\n", k_b);
    }
    print_state(root_id, true);
    print_mutex.unlock();*/
    return succ;
}

template <typename Key, typename T>
bool btree_wrapper<Key, T>::update(const char *key, size_t key_sz, const char *value, size_t value_sz)
{
    Key k = *reinterpret_cast<Key *>(const_cast<char *>(key));
    T v = *reinterpret_cast<T *>(const_cast<char *>(value));
    int cur_id;
    {
        std::shared_lock lock(root_mutex);
        cur_id = root_id;
    }
    large_mutex[cur_id]->lock_shared();
    while (!is_leaf[cur_id])
    {
        btree_node *node = get_node(cur_id);
        // printf("0 %lld %lld/%lld\n", cur_id, node->num_item, node_cap);
        int pre_id = cur_id;
        cur_id = get_nxt_id(node, k);
        if (!is_leaf[cur_id])
        {
            large_mutex[cur_id]->lock_shared();
        }
        else
        {
            large_mutex[cur_id]->lock();
        }
        large_mutex[pre_id]->unlock_shared();
        delete node;
    }
    btree_data *data = get_data(cur_id);
    bool succ;
    succ = set_nxt_val(data, k, v);
    set_data(cur_id, data);
    large_mutex[cur_id]->unlock();
    delete data;
    return succ;
}

template <typename Key, typename T>
bool btree_wrapper<Key, T>::remove(const char *key, size_t key_sz)
{
    Key k = *reinterpret_cast<Key *>(const_cast<char *>(key));
    int cur_id;
    {
        std::shared_lock lock(root_mutex);
        cur_id = root_id;
    }
    large_mutex[cur_id]->lock_shared();
    while (!is_leaf[cur_id])
    {
        btree_node *node = get_node(cur_id);
        // printf("0 %lld %lld/%lld\n", cur_id, node->num_item, node_cap);
        int pre_id = cur_id;
        cur_id = get_nxt_id(node, k);
        if (!is_leaf[cur_id])
        {
            large_mutex[cur_id]->lock_shared();
        }
        else
        {
            large_mutex[cur_id]->lock();
        }
        large_mutex[pre_id]->unlock_shared();
        delete node;
    }
    btree_data *data = get_data(cur_id);
    bool succ;
    succ = del_nxt_val(data, k);
    set_data(cur_id, data);
    large_mutex[cur_id]->unlock();
    delete data;
    return succ;
}

template <typename Key, typename T>
int btree_wrapper<Key, T>::scan(const char *key, size_t key_sz, int scan_sz, char *&values_out)
{
    std::shared_lock lock(mutex_);
    return scan_sz;
}

#endif