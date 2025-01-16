#include <iostream>
#include <vector>
#include <chrono>
#include <thread>
#include <cassert>
#include <cstring>

#include "stdio.h"
#include "stdlib.h"

void multi_memcpy_imp(const std::vector<size_t> &in, const std::vector<size_t> &out, 
                      const size_t start, const size_t end) {
  for (size_t i = start; i < end; ++i) {
    memcpy((void *)&out[i], (void *)&in[i], sizeof(size_t));
  }
}

void multi_memcpy(const std::vector<size_t> in, const std::vector<size_t> out, const size_t threads_num) {
  std::vector<std::thread> thread_vector;
  assert(in.size() == out.size());
  size_t rem = in.size() % threads_num;
  size_t partition = in.size() / threads_num;
  for (size_t i = 0; i < threads_num; ++i) {
    size_t start = i * partition;
    thread_vector.emplace_back(std::thread(multi_memcpy_imp, in, out, start, start+partition));
  }
  if (rem > 0) {
    size_t start = threads_num * partition;
    thread_vector.emplace_back(std::thread(multi_memcpy_imp, in, out, start, start + rem));
  }
  for (size_t i = 0; i < thread_vector.size(); ++i) {
    thread_vector[i].join();
  }
}

void simple_memcpy(const std::vector<size_t> in, const std::vector<size_t> out) {
  assert(in.size() == out.size());
  size_t size = in.size();
  for (size_t i = 0; i < size; ++i) {
    memcpy((void *)&out[i], (void *)&in[i], sizeof(size_t));
  }
}

int main(int argc, char **argv){
  size_t features_num = 300;
  size_t threads_num = 1;
  std::vector<size_t> in;
  std::vector<size_t> out;
  in.reserve(features_num);
  out.reserve(features_num);
  for (size_t i = 0; i < features_num; ++i) {
    in[i] = random();
  }
  std::cout << "特征数：" << features_num << " 线程数：" << threads_num << std::endl;
  auto tick = std::chrono::high_resolution_clock::now();
  multi_memcpy(in, out, threads_num);
  auto tock = std::chrono::high_resolution_clock::now();
  auto diff =
      std::chrono::duration_cast<std::chrono::nanoseconds>(tock - tick).count();
  std::cout << "multi_memcpy end diff=" << diff << " 十亿分之一秒" << std::endl;

  tick = std::chrono::high_resolution_clock::now();
  simple_memcpy(in, out);
  tock = std::chrono::high_resolution_clock::now();
  diff =
      std::chrono::duration_cast<std::chrono::nanoseconds>(tock - tick).count();
  std::cout << "simple_memcpy end diff=" << diff << " 十亿分之一秒" << std::endl;
}