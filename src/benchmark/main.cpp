// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

/*
 * Simple benchmark that runs a mixture of point lookups and inserts on ALEX.
 */

#include "../core/alex.h"

#include <iomanip>
#include <cstdint>
#include "flags.h"
#include "utils.h"
#include <memory>

// Modify these if running your own workload
#define KEY_TYPE uint64_t
#define PAYLOAD_TYPE double
// 添加一个打印节点信息的函数
struct NodeInfo {
    double slope;
    double intercept;
    KEY_TYPE min_key;
    KEY_TYPE max_key;
};

std::vector<NodeInfo> all_node_info;

int count_lines(const std::string& file_path) {
    std::ifstream infile(file_path.c_str());
    int count = std::count(std::istreambuf_iterator<char>(infile),
                           std::istreambuf_iterator<char>(), '\n');
    return count;
}


void print_node_info(alex::AlexNode<KEY_TYPE, PAYLOAD_TYPE>* node, int node_num) {
  std::cout << "Node #" << node_num << " information:\n";

  if (node->is_leaf_) {
    auto datanode = static_cast<alex::AlexDataNode<KEY_TYPE, PAYLOAD_TYPE>*>(node);
    
    if (datanode->key_slots_ != nullptr) {
      std::cout << "  - Data Node - First key: " << datanode->key_slots_[1] << "\n";
    }
    // std::cout << "  - Number of shifts: " << datanode->num_shifts_ << "\n";
    // std::cout << "  - Number of lookups: " << datanode->num_lookups_ << "\n";
    // std::cout << "  - Number of resizes: " << datanode->num_resizes_ << "\n";
    NodeInfo info;
    info.slope = datanode->model_.a_;
    info.intercept = datanode->model_.b_;
    info.min_key = datanode->min_key_;
    info.max_key = datanode->max_key_;
    
    all_node_info.push_back(info);
  } else {
    auto modelnode = static_cast<alex::AlexModelNode<KEY_TYPE, PAYLOAD_TYPE>*>(node);
    std::cout << "  - Model Node - Number of children: " << modelnode->num_children_ << "\n";
  }
}

void export_node_info_to_file(const std::string& filename) {
    std::ofstream outfile(filename.c_str());
    for (const auto& info : all_node_info) {
        outfile << info.slope << "," << info.intercept << "," << info.min_key << "," << info.max_key << "\n";
    }
    outfile.close();
}

KEY_TYPE* generateKeys(std::map<std::string, std::string>& flags, int& total_num_keys, PAYLOAD_TYPE usr_id) {
  std::string keys_file_type = get_required(flags, "keys_file_type");

  // Construct the user-specific file path
  std::string user_file_path = "./avg/user_" + std::to_string(static_cast<int>(usr_id)) + ".txt";

  // Count lines in the user-specific file to determine total_num_keys
  total_num_keys = count_lines(user_file_path);

  // Load keys
  KEY_TYPE* keys = new KEY_TYPE[total_num_keys];
  if (keys_file_type == "binary") {
    load_binary_data(keys, total_num_keys, user_file_path);
  } else if (keys_file_type == "text") {
    load_text_data(keys, total_num_keys, user_file_path);
  } else {
    std::cerr << "--keys_file_type must be either 'binary' or 'text'" << std::endl;
    delete[] keys; // 在错误情况下，不要忘记释放已分配的内存
    return nullptr;
  }

  return keys;
}
std::unique_ptr<std::pair<KEY_TYPE, PAYLOAD_TYPE>[]> buildvalue(KEY_TYPE* keys, int init_num_keys, PAYLOAD_TYPE usr_id) {
    std::unique_ptr<std::pair<KEY_TYPE, PAYLOAD_TYPE>[]> values(new std::pair<KEY_TYPE, PAYLOAD_TYPE>[init_num_keys]);
    for (int i = 0; i < init_num_keys; i++) {
        values[i].first = keys[i];
        values[i].second = usr_id;
    }
    return values;
}

std::unique_ptr<alex::Alex<KEY_TYPE, PAYLOAD_TYPE>> buildIndex(
    std::map<std::string, std::string>& flags, 
    std::unique_ptr<std::pair<KEY_TYPE, PAYLOAD_TYPE>[]> values, 
    int total_num_keys) {
  
  // Create ALEX and bulk load
  auto index = std::make_unique<alex::Alex<KEY_TYPE, PAYLOAD_TYPE>>();
  std::sort(values.get(), values.get() + total_num_keys,
            [](auto const& a, auto const& b) { return a.first < b.first; });
  index->bulk_load(values.get(), total_num_keys);
  return index;
}

void insertKeysForUser(alex::Alex<KEY_TYPE, PAYLOAD_TYPE>* index, 
                       std::map<std::string, std::string>& flags, 
                       PAYLOAD_TYPE usr_id) {
    // Construct the user-specific file path
    std::string user_file_path = "./avg/user_" + std::to_string(static_cast<int>(usr_id)) + ".txt";

    // Calculate number of keys for the user by counting lines in the file
    int num_keys_for_user = count_lines(user_file_path);

    // Generate keys for the given user
    auto keys_for_user = generateKeys(flags, num_keys_for_user, usr_id);
    
    // Generate values for the given user
    auto values_for_user = buildvalue(keys_for_user, num_keys_for_user, usr_id);

    // Start the insertion process
    auto inserts_start_time = std::chrono::high_resolution_clock::now();

    for (int j = 0; j < num_keys_for_user; j++) {
        try {
            index->insert(keys_for_user[j], values_for_user[j].second);
        } catch (std::bad_alloc& ba) {
            std::cerr << "Failed to insert key for user " << usr_id 
                      << " at position " << j << ": " << ba.what() << '\n';
            delete[] keys_for_user;  // Remember to free memory in case of exceptions
            return;
        }
    }

    auto inserts_end_time = std::chrono::high_resolution_clock::now();
    double insert_time = std::chrono::duration_cast<std::chrono::nanoseconds>(
                             inserts_end_time - inserts_start_time).count();
    std::cout << "Time taken to insert keys for user " << usr_id << ": " 
              << insert_time << " nanoseconds" << std::endl;

    delete[] keys_for_user;
}

                       
/*
 * Required flags:
 * --keys_file              path to the file that contains keys
 * --keys_file_type         file type of keys_file (options: binary or text)
 * --init_num_keys          number of keys to bulk load with
 * --total_num_keys         total number of keys in the keys file
 * --batch_size             number of operations (lookup or insert) per batch
 *
 * Optional flags:
 * --insert_frac            fraction of operations that are inserts (instead of
 * lookups)
 * --lookup_distribution    lookup keys distribution (options: uniform or zipf)
 * --time_limit             time limit, in minutes
 * --print_batch_stats      whether to output stats for each batch
 */
int main(int argc, char* argv[]) {
  
  auto flags = parse_flags(argc, argv);
  std::string keys_file_type = get_required(flags, "keys_file_type");
  PAYLOAD_TYPE usr_id = stoi(get_required(flags, "init_usr_id"));
  
  int total_num_keys;
  auto keys = generateKeys(flags, total_num_keys, usr_id);
  
  auto values = buildvalue(keys, total_num_keys, usr_id);
  auto index = buildIndex(flags, std::move(values), total_num_keys);
  std::cout << "bulk over" << std::endl; 

  // Run workload
  int i = total_num_keys;
  // long long cumulative_inserts = 0;
  // long long cumulative_lookups = 0;
  // // auto batch_size = stoi(get_required(flags, "batch_size"));
  // // auto insert_frac = stod(get_with_default(flags, "insert_frac", "0.5"));
  // // int num_inserts_per_batch = static_cast<int>(batch_size * insert_frac);
  // // int num_lookups_per_batch = batch_size - num_inserts_per_batch;
  // double cumulative_insert_time = 0;
  // double cumulative_lookup_time = 0;

  auto workload_start_time = std::chrono::high_resolution_clock::now();
  int batch_no = 0;
  // PAYLOAD_TYPE sum = 0;
  std::cout << std::scientific;
  std::cout << std::setprecision(3);
  alex::Alex<KEY_TYPE, PAYLOAD_TYPE>::NodeIterator node_it(index.get());
  int num = 0;
  for (; !node_it.is_end(); node_it.next()) {
    auto node = node_it.current();
    if (node->is_leaf_) {
      num++;
      print_node_info(node, num);
    }
  }
  std::cout << "Total number of data nodes: " << num << std::endl;

  // Export node information to a file
  export_node_info_to_file("node_info.txt");
  std::cout << "Node information exported to node_info.txt" << std::endl;




  //sector2
  std::vector<double> insertion_times(9, 0.0);

  for (int user = 1; user <= 9; user++) {
    auto start_time = std::chrono::high_resolution_clock::now();
    insertKeysForUser(index.get(), flags, user);
    auto end_time = std::chrono::high_resolution_clock::now();
    insertion_times[user - 1] = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                    end_time - start_time).count();

    // Update all_node_info after each insertion
    all_node_info.clear();
    alex::Alex<KEY_TYPE, PAYLOAD_TYPE>::NodeIterator node_it(index.get());
    int num = 0;
    for (; !node_it.is_end(); node_it.next()) {
      auto node = node_it.current();
      if (node->is_leaf_) {
        num++;
        print_node_info(node, num);
      }
    }

    // Export node information after each insertion
    std::string filename = "node_info_after_user_" + std::to_string(user) + ".txt";
    export_node_info_to_file(filename);
    std::cout << "Node information after inserting keys for user " << user 
              << " exported to " << filename << std::endl;
  }

  // Print insertion times
  for (int user = 1; user <= 9; user++) {
    std::cout << "Time taken to insert keys for user " << user << ": " 
              << insertion_times[user - 1] << " nanoseconds" << std::endl;
  }





  delete[] keys;

  return 0;
  
}

