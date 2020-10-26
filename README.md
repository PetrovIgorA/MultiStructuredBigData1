# K-Means for MapReduce (Var3)

Course: Managing multi-structured big data  
Lector: Bryuhov D. O.

### Description
The program uses two-dimensional integer coordinates (`data_*.txt`) as input. The origin centers are specified in the `center_*.txt` files. The number of lines in the file means the number k in the K-Means algorithm. The algorithm uses three metrics: Euclidean, Manhattan and Chebyshev.

### Run

```shell
python main.py path/to/data
```

##### Examples

```shell
python main.py "C:\\this_repo\\data\\data_1.txt"
```

```shell
python main.py data/data_simple.txt
```
