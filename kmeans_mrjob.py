"""@package kmeans_mrjob

Algorithm K-Means for MapReduce
"""

from mrjob.job import MRJob
import math
import functools
import os

WORKSPACE_PATH = os.path.dirname(__file__)
DEFAULT_DEBUG_PATH = WORKSPACE_PATH + "/debug.txt"

# Debug function
def debug(*args, debug_filename=DEFAULT_DEBUG_PATH, sep=' ', end='\n'):
    """
    print()-like function for debugging
    """
    with open(debug_filename, 'a') as file:
        for arg in args:
            file.write(str(arg) + sep)
        file.write(end)


def distance(point_1: list, point_2: list, metric='manhattan'):
    """
    Distance between two points.
    """

    if metric == 'manhattan':
        return functools.reduce(lambda x, y: x + y, map(lambda x, y: abs(x - y), point_1, point_2))
    elif metric == 'euclid':
        return functools.reduce(lambda x, y: x + y, map(lambda x, y: (x - y) ** 2, point_1, point_2))
    elif metric == 'chebyshev':
        return functools.reduce(max, map(lambda x, y: abs(x - y), point_1, point_2))
    else:
        raise ValueError("Incorrect metric")

# override decorator
def override(super_class):
    def overrider(method):
        assert(method.__name__ in dir(super_class))
        return method
    return overrider

class KMeansMRJob(MRJob):
    """
    Main class for MapReduce working for K-means algorithm
    """

    metric = 'manhattan'

    DEFAULT_CENTERS_PATH = WORKSPACE_PATH + "/data/centers.txt"
    __OLD_CENTERS_PATH = WORKSPACE_PATH + "/old_centers.txt"
    __NEW_CENTERS_PATH = WORKSPACE_PATH + "/new_centers.txt"

    @staticmethod
    def get_centers(filename=__OLD_CENTERS_PATH) -> list:
        """
        Get centers coordinates from file
        """

        centers = []
        if os.path.exists(filename):
            with open(filename, 'r') as file:
                for point_str in file.read().split('\n'):
                    if len(point_str) != 0:
                        centers.append(list(map(int, point_str.split(' '))))
            centers.sort(key=lambda point : point[0])
        return centers

    @staticmethod
    def set_centers(centers: list, filename=__OLD_CENTERS_PATH):
        """
        Set centers coordinates to file
        """

        with open(filename, 'w') as file:
            for point in centers:
                file.write(str(point[0]) + ' ' + str(point[1]) + ' ' + str(point[2]) + '\n')
    
    @staticmethod
    def add_center(class_id: int, center: list, filename=__NEW_CENTERS_PATH):
        """
        Add center coordinates to file
        """

        with open(filename, 'a') as file:
            file.write(str(class_id) + ' ' + str(center[0]) + ' ' + str(center[1]) + '\n')

    START_MIN_DISTANCE = 1000000000

    # Map stage
    @override(MRJob)
    def mapper(self, _, data_lines):
        """
        Mapper for K-Means algorithm
        """

        centers = self.get_centers()

        # yield in loop for each point from data
        for point_str in data_lines.split('\n'):
            # Get point from string
            point = list(map(int, point_str.split(' ')))

            # Initialization for start of class searching
            min_distance = self.START_MIN_DISTANCE
            nearest_class = 0
            # Nearest class searching
            for class_id in range(len(centers)):
                distance_i = distance(point, centers[class_id][1:3], metric=self.metric)
                if distance_i < min_distance:
                    nearest_class = class_id
                    min_distance = distance_i
            # Mapper output
            yield nearest_class, point

    # Reduce stage
    @override(MRJob)
    def reducer(self, class_id, points):
        """
        Reducer for K-Means algorithm
        """

        # Calculate mean for class
        mean_point = [0.0, 0.0]
        count = 0
        for point in points:
            mean_point[0] += point[0]
            mean_point[1] += point[1]
            count += 1        
        mean_point = list(map(lambda x: int(round(x / count)) , mean_point))
        # Save new center
        self.add_center(class_id, mean_point)
        # Reducer output
        yield class_id, mean_point

    def debug(self, is_debug: bool, step: int):
        """
        Print intermediate results into debug file
        """

        if is_debug:
            debug("OLD", step, self.get_centers(self.__OLD_CENTERS_PATH))
            debug("NEW", step, self.get_centers(self.__NEW_CENTERS_PATH), end='\n\n')
    
    def one_step(self, step: int, is_debug: bool):
        """
        One step in main MapReduce loop.
        Return True if new centers are equal to old centers
        """

        # Remove irrelevant file
        if step != 0:
            os.remove(self.__NEW_CENTERS_PATH)
        # MapReduce calculate k-means
        self.run()
        # Debug info
        self.debug(is_debug, step)
        # Check centers changing
        is_end = self.get_centers(self.__OLD_CENTERS_PATH) == self.get_centers(self.__NEW_CENTERS_PATH)
        # Save new center as old for next step
        self.set_centers(self.get_centers(filename=self.__NEW_CENTERS_PATH))

        return is_end

    def full_run(self, max_steps_count=0, start_centers_path=DEFAULT_CENTERS_PATH, is_debug=False):
        """
        Full running with several steps
        """

        if max_steps_count < 0:
            raise ValueError("max_steps_count must be >= 0")

        if is_debug:
            debug("-- Metric is " + self.metric + " -- Start centers: " + start_centers_path, sep='', end='\n\n')

        # Preparing temporary files
        self.set_centers(self.get_centers(start_centers_path))
        # Steps count
        step = 0
        
        # Calculate until all new centers are not changed
        if max_steps_count == 0:
            # Main loop
            while not self.one_step(step, is_debug):
                step += 1

        # Calculate steps_count steps
        if max_steps_count > 0:
            # Main loop
            while step < max_steps_count and not self.one_step(step, is_debug):
                step += 1
            # For correct value of steps
            if step == max_steps_count:
                step -= 1

        # Clear workspace
        os.remove(self.__OLD_CENTERS_PATH)
        os.remove(self.__NEW_CENTERS_PATH)
        
        if is_debug:
            debug("Steps count: " + str(step + 1), sep='', end='\n\n')
        return step + 1
