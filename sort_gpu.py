from numba import cuda

@cuda.jit
def merge_kernel(arr, temp, width, n, reverse):
    idx = cuda.grid(1)
    start = 2 * idx * width
    mid = min(start + width, n)
    end = min(start + 2 * width, n)

    if start < mid and mid < end:
        i = start
        j = mid
        k = start

        while i < mid and j < end:
            if reverse:
                if arr[i] >= arr[j]:
                    temp[k] = arr[i]
                    i += 1
                else:
                    temp[k] = arr[j]
                    j += 1
            else:
                if arr[i] <= arr[j]:
                    temp[k] = arr[i]
                    i += 1
                else:
                    temp[k] = arr[j]
                    j += 1
            k += 1

        while i < mid:
            temp[k] = arr[i]
            i += 1
            k += 1

        while j < end:
            temp[k] = arr[j]
            j += 1
            k += 1

        for i in range(start, end):
            arr[i] = temp[i]

def merge_sort(arr, reverse=False):
    n = len(arr)
    temp = cuda.device_array_like(arr)
    threads_per_block = 1024
    blocks_per_grid = (n + threads_per_block - 1) // threads_per_block
    width = 1
    d_arr = cuda.to_device(arr)
    while width < n:
        merge_kernel[blocks_per_grid, threads_per_block](d_arr, temp, width, n, reverse)
        width *= 2

    return arr
