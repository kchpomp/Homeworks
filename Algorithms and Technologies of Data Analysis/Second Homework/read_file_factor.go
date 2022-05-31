package main

import (
	"fmt"
	"io/ioutil"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

var mutex = &sync.Mutex{}

func factorizer(n int) []int {

	var result []int

	var yd = 2

	for n%yd == 0 {
		result = append(result, yd)
		n = n / 2
	}
	for i := 3; i*i <= n; i = i + 2 {
		for n%i == 0 {
			result = append(result, i)
			n = n / i
		}
	}

	if n > 2 {
		result = append(result, n)
	}

	return result
}

func summarizer(arr []int) []int {

	mutex.Lock()
	var res_arr []int

	for i := 0; i < len(arr); i++ {
		res_arr_temp := append(factorizer(arr[i]))

		for j := 0; j < len(res_arr_temp); j++ {
			res_arr = append(res_arr, res_arr_temp[j])
		}
	}
	mutex.Unlock()
	fmt.Println("Array size is: ", len(res_arr))
	return res_arr
}

func main() {
	start := time.Now()
	// Read a string from file, then convert it to an array
	a, err := ioutil.ReadFile("C:/Users/user/Downloads/line_numbers_golang")
	if err != nil {
		panic(err)
	}
	str := string(a)
	str_split := strings.Fields(str)
	fmt.Println("Length of the final string", len(str_split))
	fmt.Println("Type of this string is", reflect.TypeOf(str_split))

	var data_slice = []int{}

	for _, i := range str_split {
		j, err := strconv.Atoi(i)
		if err != nil {
			panic(err)
		}
		data_slice = append(data_slice, j)
	}
	fmt.Println("Length of the array of integers is: ", len(data_slice))
	fmt.Println("Type of the variable is: ", reflect.TypeOf(data_slice))

	b := data_slice[0:250]
	c := data_slice[250:500]
	d := data_slice[500:750]
	e := data_slice[750:1000]
	f := data_slice[1000:1250]
	g := data_slice[1250:1500]
	h := data_slice[1500:1750]
	z := data_slice[1750:2000]

	//fmt.Println("Length of the first slice: ", len(b))
	//fmt.Println("Length of the second slice: ", len(c))
	//fmt.Println("Length of the third slice: ", len(d))
	//fmt.Println("Length of the fourth slice: ", len(e))
	//fmt.Println("Length of the fifth slice: ", len(f))
	//fmt.Println("Length of the sixth slice: ", len(g))
	//fmt.Println("Length of the seventh slice: ", len(h))
	//fmt.Println("Length of the eighth slice: ", len(z))

	//var res_arr []int
	//for i := 0; i < len(data_slice); i++ {
	//	res_arr_temp := append(factorizer(data_slice[i]))
	//
	//	for j := 0; j < len(res_arr_temp); j++ {
	//		res_arr = append(res_arr, res_arr_temp[j])
	//	}
	//}
	//fmt.Println(len(summarizer(data_slice)))

	var wg sync.WaitGroup

	fin_len := 0

	wg.Add(1)
	go func() int {
		defer wg.Done()
		//fmt.Println(len(summarizer(b)))
		res_b := len(summarizer(b))
		fin_len = fin_len + res_b
		runtime.Gosched()
		return res_b
	}()

	wg.Add(1)
	go func() int {
		defer wg.Done()
		//fmt.Println(len(summarizer(c)))
		res_c := len(summarizer(c))
		fin_len = fin_len + res_c
		runtime.Gosched()
		return res_c
	}()

	wg.Add(1)
	go func() int {
		defer wg.Done()
		//fmt.Println(len(summarizer(d)))
		res_d := len(summarizer(d))
		fin_len = fin_len + res_d
		runtime.Gosched()
		return res_d
	}()

	wg.Add(1)
	go func() int {
		defer wg.Done()
		//fmt.Println(len(summarizer(e)))
		res_e := len(summarizer(e))
		fin_len = fin_len + res_e
		runtime.Gosched()
		return res_e
	}()

	wg.Add(1)
	go func() int {
		defer wg.Done()
		//fmt.Println(len(summarizer(f)))
		res_f := len(summarizer(f))
		fin_len = fin_len + res_f
		runtime.Gosched()
		return res_f
	}()

	wg.Add(1)
	go func() int {
		defer wg.Done()
		//fmt.Println(len(summarizer(g)))
		res_g := len(summarizer(g))
		fin_len = fin_len + res_g
		runtime.Gosched()
		return res_g
	}()

	wg.Add(1)
	go func() int {
		defer wg.Done()
		//fmt.Println(len(summarizer(h)))
		res_h := len(summarizer(h))
		fin_len = fin_len + res_h
		runtime.Gosched()
		return res_h
	}()

	wg.Add(1)
	go func() int {
		defer wg.Done()
		//fmt.Println(len(summarizer(z)))
		res_z := len(summarizer(z))
		fin_len = fin_len + res_z
		runtime.Gosched()
		return res_z
	}()

	wg.Wait()
	fmt.Println("Final length of factorized file is: ", fin_len)
	elapsed := time.Since(start)

	fmt.Println("It took", elapsed, "to complete program")
}
