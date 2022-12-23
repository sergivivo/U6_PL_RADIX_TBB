#include <oneapi/tbb/info.h>
#include <oneapi/tbb/parallel_scan.h>
#include <cassert>
#include <iostream>
#include <cmath>

// https://docs.oneapi.io/versions/latest/onetbb/tbb_userguide/Migration_Guide/Task_Scheduler_Init.html

#include <tbb/tbb.h>
#include "oneapi/tbb/blocked_range.h"
#include "oneapi/tbb/parallel_for.h"

using namespace oneapi;

// Find max value using reduce
uint32_t max_value(const std::vector<uint32_t> x) {
   auto max = tbb::parallel_reduce(
        tbb::blocked_range<uint16_t>(0, x.size()),
        0,
        [&](tbb::blocked_range<uint16_t> r, uint32_t max_acc) {
            for (uint16_t i = r.begin(); i < r.end(); ++i){
                max_acc = max_acc > x[i] ? max_acc : x[i];
            }
            return max_acc;
        },
        [](uint32_t lhs, uint32_t rhs) -> uint32_t {
            return lhs > rhs ? lhs : rhs;
        }
    );
    return max;
}

// Map mask check
void map_mask(std::vector<uint32_t> &in, std::vector<uint8_t> &out, uint32_t mask) {
    oneapi::tbb::parallel_for(
        oneapi::tbb::blocked_range<uint16_t>(0, in.size()),
        [&](tbb::blocked_range<uint16_t> r) {
            for (int i=r.begin(); i<r.end(); i++){
                out[i] = (in[i] & mask) ? 1 : 0;
            }
        }
    );
}

// Scan counting ones and zeroes
void scan_count(
    std::vector<uint8_t> &in,    // An array filled of zeroes and ones
    std::vector<uint16_t> &out0, // Output array for the scan counting 0
    std::vector<uint16_t> &out1  // Output array for the scan counting 1
) {

    tbb::parallel_scan(
        tbb::blocked_range<uint16_t>(0,in.size()),
        0,
        [&](const tbb::blocked_range<uint16_t> &r,
            uint16_t sum,
            bool is_final_scan) -> uint16_t {
                uint16_t tmp = sum;
                for (int i=r.begin(); i!= r.end(); i++){
                    tmp += in[i];
                    if(is_final_scan) {
                        out1[i] = tmp;
                        out0[i] = i + 1 - tmp;
                    }
                }
                return tmp;
            },
            [&](uint8_t a, uint8_t b){
                return a + b;
            }
    );
}

// Map input elements to specific indexes at output vector
void map_index(
    std::vector<uint32_t> &in,
    std::vector<uint8_t>  &map,
    std::vector<uint16_t> &scan1,
    std::vector<uint16_t> &scan2,
    std::vector<uint32_t> &out
) {
    oneapi::tbb::parallel_for(
        oneapi::tbb::blocked_range<uint16_t>(0, in.size()),
        [&](tbb::blocked_range<uint16_t> r) {
            uint16_t i, j;
            for (i=r.begin(); i<r.end(); i++){
                if(map[i] == 0) {
                    // Map at the left of the vector
                    j = scan1[i] - 1;
                    out[j] = in[i];
                } else {
                    // Map at the right of the vector
                    j = scan1[scan1.size()-1] + scan2[i] - 1;
                    out[j] = in[i];
                }
            }
        }
    );
}

// Radix sorting function
std::vector<uint32_t> radix_sort(const std::vector<uint32_t> input) {
    /*
    Ejemplo:
        Check with mask 0x00000001:
            input  = [5, 4, 3, 2, 1]

            map    = [1, 0, 1, 0, 1] // Map apply mask

            scan1  = [0, 1, 1, 2, 2] // Scan count zeroes
            scan2  = [1, 1, 2, 2, 3] // Scan count ones

            Para cada elemento de input:
                Si su valor en map es 0:
                    Indice output indicado en scan1
                Si su valor en map es 1:
                    Indice output indicado en scan2 
                    (sumándole al índice máximo de scan1)

            output = [4, 2, 5, 3, 1]
    */

    const uint16_t N = input.size();

    std::vector<uint8_t> map(N); // Map function applies mask

    std::vector<uint16_t> scan1(N); // Scan count ones
    std::vector<uint16_t> scan2(N); // Scan count zeroes

    // Alterna entre arrays auxiliares cada iteración
    std::vector<uint32_t> aux1(N);
    std::vector<uint32_t> aux2(N);
    std::vector<uint32_t> *inp, *out;

    // Array copy
    for(uint16_t idx = 0; idx < N; idx++) {
        aux1[idx] = input[idx];
    }

    // Count digits for the max value
    uint32_t max = max_value(input); // O(log(n))
    uint8_t digits = 0;
    while(max > 0) { 
        max >>= 1;
        digits++;
    }


    // Solution
    out = &aux1;
    for(uint8_t d = 0; d < digits; d++) {
        // Switch input and output arrays every iteration
        inp = d % 2 ? &aux2 : &aux1;
        out = d % 2 ? &aux1 : &aux2;

        uint32_t mask = 1 << d;

        // Map mask check - O(1)
        map_mask(*inp, map, mask);

        // Scan count zeroes and ones - O(n*log(n))
        scan_count(map, scan1, scan2);

        // Map input elements to specific indexes at output vector - O(1)
        map_index(*inp, map, scan1, scan2, *out);

    } // log2(n) iterations

    // Tiempo total tras paralelizar:
    //     Reduce:  Iterations:  Map:      Scan:       Map:     Result:
    //     O(log(n)) + log(n) * (O(1) + O(n*log(n)) + O(1)) = O(n*log(n))
    return *out;
}

int main() {
    // Get the default number of threads
    int num_threads = oneapi::tbb::info::default_concurrency();
    std::cout << "Default concurrency " << num_threads << std::endl;

    const std::vector<uint32_t> input = {32, 12, 5, 2, 64, 12, 4, 84, 1, 3};
    
    std::cout << "Input vector: " ;
    for (auto i: input)
        std::cout << i << ' ';
    std::cout << std::endl;

    std::vector<uint32_t> output = radix_sort(input);

    std::cout << "Output vector: " ;
    for (auto i: output)
        std::cout << i << ' ';
    std::cout << std::endl;

    return 0;
}
