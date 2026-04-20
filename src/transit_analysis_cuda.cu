#include "transit_analysis.h"

#include <cuda_runtime.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>

typedef unsigned long long device_counter_t;

static trip_record_t *d_records = NULL;
static device_counter_t *d_departures_per_bucket = NULL;
static device_counter_t *d_route_type_counts = NULL;
static device_counter_t *d_total_departures = NULL;
static device_counter_t *d_peak_departures = NULL;
static device_counter_t *d_offpeak_departures = NULL;
static device_counter_t *d_headway_count = NULL;
static device_counter_t *d_bunch_count = NULL;
static device_counter_t *d_wide_gap_count = NULL;
static device_counter_t *d_trip_count = NULL;
static device_counter_t *d_stop_count_sum = NULL;
static double *d_trip_duration_sum = NULL;
static double *d_trip_duration_sq_sum = NULL;
static double *d_headway_sum = NULL;
static double *d_headway_sq_sum = NULL;
static size_t d_capacity_records = 0;
static uint32_t d_city_count = 0;
static uint32_t d_bucket_count = 0;
static uint32_t d_route_type_bins = 0;

static void cuda_check(cudaError_t code, const char *context)
{
    if (code != cudaSuccess) {
        std::fprintf(stderr, "%s failed: %s\n", context, cudaGetErrorString(code));
        std::exit(1);
    }
}

__device__ __forceinline__ uint32_t normalize_to_day_device(uint32_t seconds)
{
    return seconds % 86400U;
}

__device__ __forceinline__ int transit_is_peak_window_device(uint32_t normalized_start_secs)
{
    return ((normalized_start_secs >= TRANSIT_PEAK_AM_START &&
             normalized_start_secs < TRANSIT_PEAK_AM_END) ||
            (normalized_start_secs >= TRANSIT_PEAK_PM_START &&
             normalized_start_secs < TRANSIT_PEAK_PM_END));
}

__device__ __forceinline__ int transit_same_headway_group_device(const trip_record_t &lhs,
                                                                 const trip_record_t &rhs)
{
    return lhs.city_id == rhs.city_id &&
           lhs.route_hash == rhs.route_hash &&
           lhs.direction_id == rhs.direction_id;
}

__global__ void analyze_kernel(const trip_record_t *records,
                               size_t record_count,
                               uint32_t bucket_count,
                               uint32_t route_type_bins,
                               uint32_t bucket_width,
                               device_counter_t *departures_per_bucket,
                               device_counter_t *route_type_counts,
                               device_counter_t *total_departures,
                               device_counter_t *peak_departures,
                               device_counter_t *offpeak_departures,
                               device_counter_t *headway_count,
                               device_counter_t *bunch_count,
                               device_counter_t *wide_gap_count,
                               device_counter_t *trip_count,
                               device_counter_t *stop_count_sum,
                               double *trip_duration_sum,
                               double *trip_duration_sq_sum,
                               double *headway_sum,
                               double *headway_sq_sum)
{
    size_t tid = (size_t)blockIdx.x * (size_t)blockDim.x + (size_t)threadIdx.x;
    size_t stride = (size_t)gridDim.x * (size_t)blockDim.x;

    for (size_t i = tid; i < record_count; i += stride) {
        const trip_record_t current = records[i];
        uint32_t city_id = current.city_id;
        uint32_t route_type = current.route_type < route_type_bins
                                  ? current.route_type
                                  : (route_type_bins - 1U);
        uint32_t normalized_start = normalize_to_day_device(current.start_secs);
        uint32_t bucket = normalized_start / bucket_width;
        double duration = (double)current.duration_secs;

        if (bucket >= bucket_count) {
            bucket = bucket_count - 1U;
        }

        atomicAdd(&departures_per_bucket[(size_t)city_id * bucket_count + bucket], 1ULL);
        atomicAdd(&route_type_counts[(size_t)city_id * route_type_bins + route_type], 1ULL);
        atomicAdd(&total_departures[city_id], 1ULL);
        atomicAdd(&trip_count[city_id], 1ULL);
        atomicAdd(&stop_count_sum[city_id], (device_counter_t)current.stop_count);
        atomicAdd(&trip_duration_sum[city_id], duration);
        atomicAdd(&trip_duration_sq_sum[city_id], duration * duration);

        if (transit_is_peak_window_device(normalized_start)) {
            atomicAdd(&peak_departures[city_id], 1ULL);
        } else {
            atomicAdd(&offpeak_departures[city_id], 1ULL);
        }

        if (i > 0U) {
            const trip_record_t previous = records[i - 1U];
            if (transit_same_headway_group_device(previous, current) &&
                current.start_secs >= previous.start_secs) {
                uint32_t gap = current.start_secs - previous.start_secs;
                double gap_d = (double)gap;

                atomicAdd(&headway_count[city_id], 1ULL);
                atomicAdd(&headway_sum[city_id], gap_d);
                atomicAdd(&headway_sq_sum[city_id], gap_d * gap_d);
                if (gap < TRANSIT_BUNCH_THRESHOLD_SECONDS) {
                    atomicAdd(&bunch_count[city_id], 1ULL);
                }
                if (gap > TRANSIT_WIDE_GAP_THRESHOLD_SECONDS) {
                    atomicAdd(&wide_gap_count[city_id], 1ULL);
                }
            }
        }
    }
}

extern "C" void transit_cuda_init(int gpu_id,
                                  uint32_t city_count,
                                  uint32_t bucket_count,
                                  uint32_t route_type_bins)
{
    size_t city_slots = (size_t)city_count;
    size_t bucket_slots = city_slots * (size_t)bucket_count;
    size_t route_slots = city_slots * (size_t)route_type_bins;

    cuda_check(cudaSetDevice(gpu_id), "cudaSetDevice");

    d_city_count = city_count;
    d_bucket_count = bucket_count;
    d_route_type_bins = route_type_bins;

    cuda_check(cudaMalloc(&d_departures_per_bucket, bucket_slots * sizeof(device_counter_t)),
               "cudaMalloc(d_departures_per_bucket)");
    cuda_check(cudaMalloc(&d_route_type_counts, route_slots * sizeof(device_counter_t)),
               "cudaMalloc(d_route_type_counts)");
    cuda_check(cudaMalloc(&d_total_departures, city_slots * sizeof(device_counter_t)),
               "cudaMalloc(d_total_departures)");
    cuda_check(cudaMalloc(&d_peak_departures, city_slots * sizeof(device_counter_t)),
               "cudaMalloc(d_peak_departures)");
    cuda_check(cudaMalloc(&d_offpeak_departures, city_slots * sizeof(device_counter_t)),
               "cudaMalloc(d_offpeak_departures)");
    cuda_check(cudaMalloc(&d_headway_count, city_slots * sizeof(device_counter_t)),
               "cudaMalloc(d_headway_count)");
    cuda_check(cudaMalloc(&d_bunch_count, city_slots * sizeof(device_counter_t)),
               "cudaMalloc(d_bunch_count)");
    cuda_check(cudaMalloc(&d_wide_gap_count, city_slots * sizeof(device_counter_t)),
               "cudaMalloc(d_wide_gap_count)");
    cuda_check(cudaMalloc(&d_trip_count, city_slots * sizeof(device_counter_t)),
               "cudaMalloc(d_trip_count)");
    cuda_check(cudaMalloc(&d_stop_count_sum, city_slots * sizeof(device_counter_t)),
               "cudaMalloc(d_stop_count_sum)");
    cuda_check(cudaMalloc(&d_trip_duration_sum, city_slots * sizeof(double)),
               "cudaMalloc(d_trip_duration_sum)");
    cuda_check(cudaMalloc(&d_trip_duration_sq_sum, city_slots * sizeof(double)),
               "cudaMalloc(d_trip_duration_sq_sum)");
    cuda_check(cudaMalloc(&d_headway_sum, city_slots * sizeof(double)),
               "cudaMalloc(d_headway_sum)");
    cuda_check(cudaMalloc(&d_headway_sq_sum, city_slots * sizeof(double)),
               "cudaMalloc(d_headway_sq_sum)");
}

extern "C" int transit_cuda_get_device_count(void)
{
    int device_count = 0;
    cudaError_t status = cudaGetDeviceCount(&device_count);
    if (status != cudaSuccess) {
        return -1;
    }
    return device_count;
}

extern "C" void transit_analyze_cuda(const trip_record_t *records,
                                     size_t record_count,
                                     const trip_record_t *previous_record,
                                     int has_previous_record,
                                     uint32_t city_count,
                                     uint32_t bucket_count,
                                     uint32_t route_type_bins,
                                     analysis_stats_t *stats)
{
    trip_record_t *host_buffer = NULL;
    size_t effective_count = record_count + (has_previous_record ? 1U : 0U);
    size_t city_slots = (size_t)city_count;
    size_t bucket_slots = city_slots * (size_t)bucket_count;
    size_t route_slots = city_slots * (size_t)route_type_bins;
    uint32_t bucket_width = 86400U / bucket_count;

    if (effective_count == 0U) {
        return;
    }

    if (city_count != d_city_count ||
        bucket_count != d_bucket_count ||
        route_type_bins != d_route_type_bins) {
        std::fprintf(stderr, "CUDA analyzer received unexpected dimensions\n");
        std::exit(1);
    }

    host_buffer = (trip_record_t *)std::calloc(effective_count, sizeof(trip_record_t));
    if (host_buffer == NULL) {
        std::fprintf(stderr, "calloc failed for CUDA host staging buffer\n");
        std::exit(1);
    }

    if (has_previous_record) {
        host_buffer[0] = *previous_record;
    }
    if (record_count > 0U) {
        std::memcpy(host_buffer + (has_previous_record ? 1U : 0U),
                    records,
                    record_count * sizeof(trip_record_t));
    }

    if (effective_count > d_capacity_records) {
        if (d_records != NULL) {
            cuda_check(cudaFree(d_records), "cudaFree(d_records)");
        }
        cuda_check(cudaMalloc(&d_records, effective_count * sizeof(trip_record_t)),
                   "cudaMalloc(d_records)");
        d_capacity_records = effective_count;
    }

    cuda_check(cudaMemset(d_departures_per_bucket, 0, bucket_slots * sizeof(device_counter_t)),
               "cudaMemset(d_departures_per_bucket)");
    cuda_check(cudaMemset(d_route_type_counts, 0, route_slots * sizeof(device_counter_t)),
               "cudaMemset(d_route_type_counts)");
    cuda_check(cudaMemset(d_total_departures, 0, city_slots * sizeof(device_counter_t)),
               "cudaMemset(d_total_departures)");
    cuda_check(cudaMemset(d_peak_departures, 0, city_slots * sizeof(device_counter_t)),
               "cudaMemset(d_peak_departures)");
    cuda_check(cudaMemset(d_offpeak_departures, 0, city_slots * sizeof(device_counter_t)),
               "cudaMemset(d_offpeak_departures)");
    cuda_check(cudaMemset(d_headway_count, 0, city_slots * sizeof(device_counter_t)),
               "cudaMemset(d_headway_count)");
    cuda_check(cudaMemset(d_bunch_count, 0, city_slots * sizeof(device_counter_t)),
               "cudaMemset(d_bunch_count)");
    cuda_check(cudaMemset(d_wide_gap_count, 0, city_slots * sizeof(device_counter_t)),
               "cudaMemset(d_wide_gap_count)");
    cuda_check(cudaMemset(d_trip_count, 0, city_slots * sizeof(device_counter_t)),
               "cudaMemset(d_trip_count)");
    cuda_check(cudaMemset(d_stop_count_sum, 0, city_slots * sizeof(device_counter_t)),
               "cudaMemset(d_stop_count_sum)");
    cuda_check(cudaMemset(d_trip_duration_sum, 0, city_slots * sizeof(double)),
               "cudaMemset(d_trip_duration_sum)");
    cuda_check(cudaMemset(d_trip_duration_sq_sum, 0, city_slots * sizeof(double)),
               "cudaMemset(d_trip_duration_sq_sum)");
    cuda_check(cudaMemset(d_headway_sum, 0, city_slots * sizeof(double)),
               "cudaMemset(d_headway_sum)");
    cuda_check(cudaMemset(d_headway_sq_sum, 0, city_slots * sizeof(double)),
               "cudaMemset(d_headway_sq_sum)");

    cuda_check(cudaMemcpy(d_records,
                          host_buffer,
                          effective_count * sizeof(trip_record_t),
                          cudaMemcpyHostToDevice),
               "cudaMemcpy(d_records)");

    analyze_kernel<<<512, 256>>>(d_records,
                                 effective_count,
                                 bucket_count,
                                 route_type_bins,
                                 bucket_width,
                                 d_departures_per_bucket,
                                 d_route_type_counts,
                                 d_total_departures,
                                 d_peak_departures,
                                 d_offpeak_departures,
                                 d_headway_count,
                                 d_bunch_count,
                                 d_wide_gap_count,
                                 d_trip_count,
                                 d_stop_count_sum,
                                 d_trip_duration_sum,
                                 d_trip_duration_sq_sum,
                                 d_headway_sum,
                                 d_headway_sq_sum);
    cuda_check(cudaGetLastError(), "analyze_kernel launch");
    cuda_check(cudaDeviceSynchronize(), "cudaDeviceSynchronize");

    cuda_check(cudaMemcpy(stats->departures_per_bucket,
                          d_departures_per_bucket,
                          bucket_slots * sizeof(device_counter_t),
                          cudaMemcpyDeviceToHost),
               "cudaMemcpy(departures_per_bucket)");
    cuda_check(cudaMemcpy(stats->route_type_counts,
                          d_route_type_counts,
                          route_slots * sizeof(device_counter_t),
                          cudaMemcpyDeviceToHost),
               "cudaMemcpy(route_type_counts)");
    cuda_check(cudaMemcpy(stats->total_departures,
                          d_total_departures,
                          city_slots * sizeof(device_counter_t),
                          cudaMemcpyDeviceToHost),
               "cudaMemcpy(total_departures)");
    cuda_check(cudaMemcpy(stats->peak_departures,
                          d_peak_departures,
                          city_slots * sizeof(device_counter_t),
                          cudaMemcpyDeviceToHost),
               "cudaMemcpy(peak_departures)");
    cuda_check(cudaMemcpy(stats->offpeak_departures,
                          d_offpeak_departures,
                          city_slots * sizeof(device_counter_t),
                          cudaMemcpyDeviceToHost),
               "cudaMemcpy(offpeak_departures)");
    cuda_check(cudaMemcpy(stats->headway_count,
                          d_headway_count,
                          city_slots * sizeof(device_counter_t),
                          cudaMemcpyDeviceToHost),
               "cudaMemcpy(headway_count)");
    cuda_check(cudaMemcpy(stats->bunch_count,
                          d_bunch_count,
                          city_slots * sizeof(device_counter_t),
                          cudaMemcpyDeviceToHost),
               "cudaMemcpy(bunch_count)");
    cuda_check(cudaMemcpy(stats->wide_gap_count,
                          d_wide_gap_count,
                          city_slots * sizeof(device_counter_t),
                          cudaMemcpyDeviceToHost),
               "cudaMemcpy(wide_gap_count)");
    cuda_check(cudaMemcpy(stats->trip_count,
                          d_trip_count,
                          city_slots * sizeof(device_counter_t),
                          cudaMemcpyDeviceToHost),
               "cudaMemcpy(trip_count)");
    cuda_check(cudaMemcpy(stats->stop_count_sum,
                          d_stop_count_sum,
                          city_slots * sizeof(device_counter_t),
                          cudaMemcpyDeviceToHost),
               "cudaMemcpy(stop_count_sum)");
    cuda_check(cudaMemcpy(stats->trip_duration_sum,
                          d_trip_duration_sum,
                          city_slots * sizeof(double),
                          cudaMemcpyDeviceToHost),
               "cudaMemcpy(trip_duration_sum)");
    cuda_check(cudaMemcpy(stats->trip_duration_sq_sum,
                          d_trip_duration_sq_sum,
                          city_slots * sizeof(double),
                          cudaMemcpyDeviceToHost),
               "cudaMemcpy(trip_duration_sq_sum)");
    cuda_check(cudaMemcpy(stats->headway_sum,
                          d_headway_sum,
                          city_slots * sizeof(double),
                          cudaMemcpyDeviceToHost),
               "cudaMemcpy(headway_sum)");
    cuda_check(cudaMemcpy(stats->headway_sq_sum,
                          d_headway_sq_sum,
                          city_slots * sizeof(double),
                          cudaMemcpyDeviceToHost),
               "cudaMemcpy(headway_sq_sum)");

    std::free(host_buffer);
}

extern "C" void transit_cuda_finalize(void)
{
    if (d_records != NULL) {
        cudaFree(d_records);
        d_records = NULL;
    }
    if (d_departures_per_bucket != NULL) {
        cudaFree(d_departures_per_bucket);
        d_departures_per_bucket = NULL;
    }
    if (d_route_type_counts != NULL) {
        cudaFree(d_route_type_counts);
        d_route_type_counts = NULL;
    }
    if (d_total_departures != NULL) {
        cudaFree(d_total_departures);
        d_total_departures = NULL;
    }
    if (d_peak_departures != NULL) {
        cudaFree(d_peak_departures);
        d_peak_departures = NULL;
    }
    if (d_offpeak_departures != NULL) {
        cudaFree(d_offpeak_departures);
        d_offpeak_departures = NULL;
    }
    if (d_headway_count != NULL) {
        cudaFree(d_headway_count);
        d_headway_count = NULL;
    }
    if (d_bunch_count != NULL) {
        cudaFree(d_bunch_count);
        d_bunch_count = NULL;
    }
    if (d_wide_gap_count != NULL) {
        cudaFree(d_wide_gap_count);
        d_wide_gap_count = NULL;
    }
    if (d_trip_count != NULL) {
        cudaFree(d_trip_count);
        d_trip_count = NULL;
    }
    if (d_stop_count_sum != NULL) {
        cudaFree(d_stop_count_sum);
        d_stop_count_sum = NULL;
    }
    if (d_trip_duration_sum != NULL) {
        cudaFree(d_trip_duration_sum);
        d_trip_duration_sum = NULL;
    }
    if (d_trip_duration_sq_sum != NULL) {
        cudaFree(d_trip_duration_sq_sum);
        d_trip_duration_sq_sum = NULL;
    }
    if (d_headway_sum != NULL) {
        cudaFree(d_headway_sum);
        d_headway_sum = NULL;
    }
    if (d_headway_sq_sum != NULL) {
        cudaFree(d_headway_sq_sum);
        d_headway_sq_sum = NULL;
    }

    d_capacity_records = 0U;
    d_city_count = 0U;
    d_bucket_count = 0U;
    d_route_type_bins = 0U;
}
