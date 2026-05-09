pushd crankshaft
if [ ! -d obj ]; then mkdir obj; fi
make lib
popd
pushd build
if [ -f handle ]; then rm handle; fi
if [ ! -f CMakeCache.txt ]; then cmake ../CMakeLists.txt; fi
cmake --build .
popd
