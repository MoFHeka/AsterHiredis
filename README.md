# AsterHiredis
A redis client base on Seastar architecture.
  
  

# How To Build
To avoid linking errors, you need to include 'fPIC' compilation options when you compile Seastar. 

For changing the CFLAGS of DPDK, you need to modify file `cooking_recipe.cmake` in Seastar source code. At about `line 255` you could find a setting to `dpdk_args`, now modifying `"EXTRA_CFLAGS=-Wno-error -fcommon"` to `"EXTRA_CFLAGS=-Wno-error -fcommon -fPIC"` which will add additional CFLAGS when compile DPDK.

Cflags for Seastar can be added using the configure.py script in the official tutorial, as shown in the following example:

```bash
./configure.py --mode=release --cflags='-fPIC' --c++-dialect=c++2a --enable-dpdk
```
  
  
In addition, if you use DPDK, you need to explicitly specify the location of the DPDK. 
The DPDK location compiled by Seastar is usually located in path `$seastar_dir/build/release/_cooking/stow/dpdk`. 
Besides, you could copy entire DPDK founder into `/usr/local/stow`, it may be much more convenient.  

In AsterHiredis, you could set ``DPDK_DIR`` to specified DPDK direction. For example, `cmake -DDPDK_DIR='$seastar_dir/build/release/_cooking/stow/dpdk'`. And then, ``DPDK_DIR`` will be set to the ``CMAKE_PREFIX_PATH`` for finding DPDK package by CMake.
  
Now we can build the AsterHiredis:
```
mkdir build && cd build
cmake -DDPDK_DIR=$dpdk_dir ..
ninja -C ./ install
```
