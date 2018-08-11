# sqlite3_learning

### 1.sqlite3 version
```
3.24.0
```
### 2.config and build
```
./configure --enable-debug CFLAGS="-g -O0 -DSQLITE_PRIVATE=\"\" -DSQLITE_OMIT_SHARED_CACHE"
make all
```

### 3.build test program
```
gcc -g -O0 -o sqlite3_btree sqlite3_btree.c -I./ -I./src -L.libs -ldl -lpthread -lsqlite3 -DSQLITE_OMIT_SHARED_CACHE
```
