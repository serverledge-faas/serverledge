#include <stdint.h>
#include <stdio.h>

uint32_t fib(uint32_t n) {
  if (n <= 1)
    return n;
  return fib(n - 1) + fib(n - 2);
}

int main(int argc, char **argv) {
  uint32_t n = 30;

  uint32_t result = fib(n);
  printf("%u\n", result);
  return 0;
}
