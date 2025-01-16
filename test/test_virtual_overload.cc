#include <iostream>
#include <list>
#include <memory>
#include <stdlib.h>

class Base {
  public: 
    Base(){};
    ~Base(){};
    virtual int test(int a, int b) = 0;
    virtual float test(float a) = 0;
};

class Impl : public Base {
  public:
    Impl(){};
    ~Impl(){};
    int test(int a, int b) override {
      return a + b;
    }
    float test(float a) override {
      return a;
    }
};

int main(int argc, char *argv[]) {
  auto impl = Impl();
  std::cout << impl.test(1,1) << std::endl;
  std::cout << impl.test(3.0) << std::endl;
  return 0;
}