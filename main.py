from kmeans_mrjob import KMeansMRJob, WORKSPACE_PATH

CENTER_SIMPLE_PATH = WORKSPACE_PATH + "/data/centers_simple.txt"
CENTER_2_PATH = WORKSPACE_PATH + "/data/centers_2.txt"
CENTER_3_PATH = WORKSPACE_PATH + "/data/centers_3.txt"

DEFAULT_MAX_STEPS_COUNT = 5

if __name__ == "__main__":
    metrics = ['euclid', 'manhattan', 'chebyshev']
    start_centers = [CENTER_SIMPLE_PATH, CENTER_2_PATH, CENTER_3_PATH]

    for center_path in start_centers:
        for metric in metrics:
            KMeansMRJob.metric=metric
            KMeansMRJob().full_run(start_centers_path=center_path, is_debug=True, max_steps_count=DEFAULT_MAX_STEPS_COUNT)
