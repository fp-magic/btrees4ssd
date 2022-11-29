#ifndef __BTREE_WRAPPER_HPP__
#define __BTREE_WRAPPER_HPP__

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

    struct btree_node
    {
        Key key[4096 / (sizeof(Key) + sizeof(uint32_t)) - 1];
        uint32_t nxt[4096 / (sizeof(Key) + sizeof(uint32_t)) - 1];
        uint32_t num_item;
    };
    struct btree_data
    {
        Key key[4096 / (sizeof(Key) + sizeof(T)) - 1];
        T val[4096 / (sizeof(Key) + sizeof(T)) - 1];
        uint32_t num_item;
    };

    class LRUCache
    {
    public:
        std::list<std::pair<size_t, btree_node *>> item_list;
        std::unordered_map<size_t, decltype(item_list.begin())> item_map;
        size_t cache_size;
        void clean(void)
        {
            while (item_map.size() > cache_size)
            {
                auto last_it = item_list.end();
                last_it--;
                item_map.erase(last_it->first);
                item_list.pop_back();
            }
        };
        LRUCache()
        {
            cache_size = 32;
        };

        void put(size_t key, btree_node *val)
        {
            auto it = item_map.find(key);
            if (it != item_map.end())
            {
                item_list.erase(it->second);
                item_map.erase(it);
            }
            item_list.push_front(std::make_pair(key, val));
            item_map.insert(std::make_pair(key, item_list.begin()));
            clean();
        };
        bool exist(size_t key)
        {
            return (item_map.count(key) > 0);
        };
        btree_node *get(size_t key)
        {
            auto it = item_map.find(key);
            item_list.splice(item_list.begin(), item_list, it->second);
            return it->second->second;
        };
    };

    class LRUCache2
    {
    public:
        std::list<std::pair<size_t, btree_data *>> item_list;
        std::unordered_map<size_t, decltype(item_list.begin())> item_map;
        size_t cache_size;
        void clean(void)
        {
            while (item_map.size() > cache_size)
            {
                auto last_it = item_list.end();
                last_it--;
                item_map.erase(last_it->first);
                item_list.pop_back();
            }
        };
        LRUCache2()
        {
            cache_size = 32;
        };

        void put(size_t key, btree_data *val)
        {
            auto it = item_map.find(key);
            if (it != item_map.end())
            {
                item_list.erase(it->second);
                item_map.erase(it);
            }
            item_list.push_front(std::make_pair(key, val));
            item_map.insert(std::make_pair(key, item_list.begin()));
            clean();
        };
        bool exist(size_t key)
        {
            return (item_map.count(key) > 0);
        };
        btree_data *get(size_t key)
        {
            auto it = item_map.find(key);
            item_list.splice(item_list.begin(), item_list, it->second);
            return it->second->second;
        };
    };

private:
    // key: 1   50  100     200     x
    // nxt: <1  <50 <100    <200    >=200
    // num_item at least 2 for a btree_node

    std::shared_mutex mutex_;
    uint32_t node_cap = 4096 / (sizeof(Key) + sizeof(uint32_t)) - 1;
    uint32_t data_cap = 4096 / (sizeof(Key) + sizeof(T)) - 1;
    std::vector<FILE *> nodes;
    std::vector<bool> is_leaf;
    std::vector<std::shared_mutex *> small_mutex;
    std::vector<std::shared_mutex *> large_mutex;
    std::shared_mutex root_mutex, print_mutex, print_small_mutex;
    std::shared_mutex new_mutex;
    uint32_t root_id;
    // only for single thread
    uint8_t cache_type = 0; // 0:no, 1:lru, 2: write buffer
    std::unordered_map<uint32_t, btree_node *> node_write_buffer2;
    std::unordered_map<uint32_t, btree_data *> data_write_buffer2;
    LRUCache node_write_buffer1;
    LRUCache2 data_write_buffer1;
    uint32_t node_buffer_size = 128;

    btree_node *get_node(uint32_t id)
    {
        btree_node *node = new btree_node;
        if (cache_type == 2)
        {
            if (node_write_buffer2.count(id))
            {
                memcpy(node, node_write_buffer2[id], sizeof(btree_node));
                return node;
            }
        }
        else if (cache_type == 1)
        {
            if (node_write_buffer1.exist(id))
            {
                memcpy(node, node_write_buffer1.get(id), sizeof(btree_node));
                return node;
            }
        }
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

    void set_node(uint32_t id, btree_node *node)
    {
        if (cache_type == 2)
        {
            node_write_buffer2[id] = node;
            if (node_write_buffer2.size() == node_buffer_size)
            {
                set_node_buffer();
            }
            return;
        }
        else if (cache_type == 1)
        {
            node_write_buffer1.put(id, node);
        }
        size_t state;
        {
            std::unique_lock lock(*small_mutex[id]);
            rewind(nodes[id]);
            state = fwrite(node, sizeof(btree_node), 1, nodes[id]);
        }
        if (cache_type == 0)
        {
            delete node;
        }

        if (state != 1)
        {
            fprintf(stderr, "btree: I/O error in set_node\n");
            abort();
        }
    }

    void set_node_single(uint32_t id, btree_node *node)
    {
        rewind(nodes[id]);
        fwrite(node, sizeof(btree_node), 1, nodes[id]);
        delete node;
    }

    void set_node_buffer()
    {
        std::vector<std::thread *> threads(node_buffer_size);
        int i = 0;
        for (auto it : node_write_buffer2)
        {
            threads[i++] = new std::thread(&btree_wrapper::set_node_single, this, it.first, it.second);
        }
        // printf("%d %d\n", i, node_buffer_size);
        // fflush(stdout);
        for (i = 0; i < node_buffer_size; ++i)
        {
            threads[i]->join();
            delete threads[i];
        }
        node_write_buffer2.clear();
        // printf("end set node buffer\n");
    }

    uint32_t init_new_node(btree_node *node = nullptr)
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
        if (cache_type == 2)
        {
            if (data_write_buffer2.count(id))
            {
                memcpy(data, data_write_buffer2[id], sizeof(btree_data));
                return data;
            }
        }
        else if (cache_type == 1)
        {
            if (data_write_buffer1.exist(id))
            {
                memcpy(data, data_write_buffer1.get(id), sizeof(btree_data));
                return data;
            }
        }
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

    void set_data(uint32_t id, btree_data *data)
    {
        if (cache_type == 2)
        {
            data_write_buffer2[id] = data;
            if (data_write_buffer2.size() == node_buffer_size)
            {
                set_data_buffer();
            }
            return;
        }
        else if (cache_type == 1)
        {
            data_write_buffer1.put(id, data);
        }
        size_t state;
        {
            std::unique_lock lock(*small_mutex[id]);
            rewind(nodes[id]);
            state = fwrite(data, sizeof(btree_data), 1, nodes[id]);
        }
        if (cache_type == 0)
        {
            delete data;
        }

        if (state != 1)
        {
            fprintf(stderr, "btree: I/O error in set_data\n");
            abort();
        }
    }

    void set_data_single(uint32_t id, btree_data *data)
    {
        rewind(nodes[id]);
        fwrite(data, sizeof(btree_data), 1, nodes[id]);
        delete data;
    }

    void set_data_buffer()
    {
        std::vector<std::thread *> threads(node_buffer_size);
        int i = 0;
        for (auto it : data_write_buffer2)
        {
            threads[i++] = new std::thread(&btree_wrapper::set_data_single, this, it.first, it.second);
        }
        for (i = 0; i < node_buffer_size; ++i)
        {
            threads[i]->join();
            delete threads[i];
        }
        data_write_buffer2.clear();
    }

    uint32_t init_new_data(btree_data *data = nullptr)
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
        uint32_t nxt = init_new_node(node_l);
        uint32_t k = node_r->key[node_cap / 2 - 1];
        memmove(node_r->key, node_r->key + node_cap / 2, (node_cap - node_cap / 2 - 1) * sizeof(Key));
        memmove(node_r->nxt, node_r->nxt + node_cap / 2, (node_cap - node_cap / 2) * sizeof(uint32_t));
        node_r->num_item = node_cap - node_cap / 2;
        insert_node_item(node_fa, k, nxt);
    }

    void down_insert_lock(std::vector<uint32_t> &x_locked, std::vector<uint32_t> &s_locked, uint32_t lock_mode, uint32_t cur_id, uint32_t pre_id = -1)
    {
        if (pre_id == -1)
        { // the first down lock
            if (lock_mode == 0)
            {
                // printf("00before slock %lld\n", cur_id);
                large_mutex[cur_id]->lock_shared();
                s_locked.push_back(cur_id);
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
                    s_locked.push_back(cur_id);
                    // printf("12slock %lld\n", cur_id);
                }
                else
                {
                    // printf("21before xlock %lld\n", cur_id);
                    large_mutex[cur_id]->lock();
                    // printf("22xlock %lld\n", cur_id);
                    x_locked.push_back(cur_id);
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

    void revert_first_down_insert_lock(std::vector<uint32_t> &x_locked, std::vector<uint32_t> &s_locked, uint32_t lock_mode, uint32_t cur_id)
    {
        if (lock_mode == 0)
        {
            // printf("00before slock %lld\n", cur_id);
            large_mutex[cur_id]->unlock_shared();
            s_locked.pop_back();
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

    bool up_insert_lock(std::vector<uint32_t> &x_locked, std::vector<uint32_t> &s_locked, uint32_t lock_mode, uint32_t cur_id, uint32_t nxt_id = -1)
    {
        if (nxt_id == -1)
        { // cur_id is root
          // do nothing
        }
        else
        {
            if (lock_mode == 0)
            {
                // printf("try lock %lld\n", nxt_id);
                // fflush(stdout);
                // may have problem here
                large_mutex[nxt_id]->unlock_shared();
                if (!large_mutex[nxt_id]->try_lock())
                {
                    // printf("try lock failed%lld\n", nxt_id);
                    // fflush(stdout);
                    s_locked.pop_back();
                    return false;
                }
                else
                {
                    // printf("41xlock %lld\n", *id_it);
                    // printf("42before un xlock %lld\n", pre_id);
                    // printf("try lock succed%lld\n", nxt_id);
                    // fflush(stdout);
                    // printf("43un xlock %lld\n", pre_id);
                    s_locked.pop_back();
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
    node->key[0] = 2e9;
    node->nxt[1] = 2;
    node->num_item = 2;
    init_new_node(node);
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
        /*{
            std::unique_lock print_lock(print_small_mutex);
            printf("find %lld failed\n", k);
        }*/
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
            if (insert(key, key_sz, value, value_sz, 0))
            {
                // printf("%lld succ in %lld time(s)\n", k, i + 1);
                return true;
            }
        }
        else
        {
            if (insert(key, key_sz, value, value_sz, 1))
            {
                // printf("%lld succ in %lld time(s)\n", k, i + 1);
                return true;
            }
        }
    }
    // printf("%lld fail", key);
    return false;
}

template <typename Key, typename T>
bool btree_wrapper<Key, T>::insert(const char *key, size_t key_sz, const char *value, size_t value_sz, size_t lock_mode)
{
    Key k = *reinterpret_cast<Key *>(const_cast<char *>(key));
    Key k_b = k;
    T v = *reinterpret_cast<T *>(const_cast<char *>(value));
    std::vector<uint32_t> cur_ids;
    std::vector<std::pair<uint32_t, btree_data *>> uncommit_data;
    std::vector<std::pair<uint32_t, btree_node *>> uncommit_node;
    std::vector<uint32_t> x_locked;
    std::vector<uint32_t> s_locked;
    bool succ = true;
    bool root_changed = false;
    // printf("k%lld v %lld mode%lld\n", k, v, lock_mode);
    //   print_state(root_id, true);
    uint32_t cur_id;
    {
        std::shared_lock lock(root_mutex);
        cur_id = root_id;
    }
    down_insert_lock(x_locked, s_locked, lock_mode, cur_id);
    /*{
        std::unique_lock print_lock(print_small_mutex);
        printf("%lld slock %lld\n", k_b, cur_id);
    }*/
    {
        std::shared_lock lock(root_mutex);
        if (cur_id != root_id)
        { // root changed
            revert_first_down_insert_lock(x_locked, s_locked, lock_mode, cur_id);
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
        down_insert_lock(x_locked, s_locked, lock_mode, cur_id, pre_id);
        /*{
            std::unique_lock print_lock(print_small_mutex);
            if (is_leaf[cur_id])
            {
                printf("%lld xlock %lld\n", k_b, cur_id);
            }
            else
            {
                printf("%lld slock %lld\n", k_b, cur_id);
            }

            printf("%lld unslock %lld\n", k_b, pre_id);
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
    }
    else
    {
        // printf("%lld 2\n",k);
        auto id_it = cur_ids.rbegin();
        btree_node *cur_node;
        do
        {
            if (up_insert_lock(x_locked, s_locked, lock_mode, cur_id, *id_it))
            {
                /*{
                    std::unique_lock print_lock(print_small_mutex);
                    printf("%lld xlock %lld\n", k_b, *id_it);
                }*/
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
        if (succ && cur_node->num_item == node_cap)
        { // root is full
            btree_node *nxt_node = new btree_node;
            nxt_node->nxt[0] = root_id;
            nxt_node->num_item = 1;
            split_node(cur_node, nxt_node);
            uint32_t new_root_id = init_new_node(nxt_node);
            // printf("%lld prelock %lld\n", k_b, new_root_id);
            large_mutex[new_root_id]->lock();
            // printf("%lld lock %lld\n", k_b, new_root_id);
            x_locked.push_back(new_root_id);

            root_mutex.lock();
            root_changed = true;
            {
                std::unique_lock print_lock(print_small_mutex);
                // printf("%lld root id change from %lld to %lld\n", k_b, root_id, new_root_id);
            }
            root_id = new_root_id;
        }
    }
    if (succ)
    {
        for (auto id_data : uncommit_data)
        {
            // printf("%lld update %lld\n", k_b, id_data.first);
            set_data(id_data.first, id_data.second);
        }
        for (auto id_node : uncommit_node)
        {
            // printf("%lld update %lld\n", k_b, id_node.first);
            set_node(id_node.first, id_node.second);
        }
    }
    for (auto it = x_locked.rbegin(); it != x_locked.rend(); ++it)
    {
        // printf("51before un xlock %lld\n", id);
        large_mutex[*it]->unlock();
        /*{
            std::unique_lock print_lock(print_small_mutex);
            //printf("%lld unxlock %lld\n", k_b, *it);
        }*/
        // printf("%lld unlock %lld\n", k_b, id);
        //   printf("52un xlock %lld\n", id);
    }
    for (auto it = s_locked.rbegin(); it != s_locked.rend(); ++it)
    {
        // printf("51before un xlock %lld\n", id);
        large_mutex[*it]->unlock_shared();
        /*{
            std::unique_lock print_lock(print_small_mutex);
            printf("%lld unslock %lld\n", k_b, *it);
        }*/
        // printf("%lld unlock %lld\n", k_b, id);
        //   printf("52un xlock %lld\n", id);
    }
    if (root_changed)
    {
        root_mutex.unlock();
    }
    if (succ)
    {
        /*print_mutex.lock();
        {
            std::unique_lock print_lock(print_small_mutex);
            printf("state of %lld\n", k_b);
        }
        print_state(root_id, true);
        print_mutex.unlock();*/
    }

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
    return succ;
}

template <typename Key, typename T>
int btree_wrapper<Key, T>::scan(const char *key, size_t key_sz, int scan_sz, char *&values_out)
{
    std::shared_lock lock(mutex_);
    return scan_sz;
}

#endif