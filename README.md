# masterdegreeprojects-mapreduce-spark

Hierarchical density-based clustering is gaining increasing attention as a powerful tool for exploratory data analysis, which can play an important role in the understanding and organization of data sets. However, its applicability to large data sets is limited because the computational complexity of hierarchical clustering methods has a quadratic lower bound in the number of objects to be clustered. The MapReduce parallelization paradigm represents a popular programming model to speed-up data mining and machine learning algorithms operating on large, possibly distributed data sets, but hierarchical clustering algorithms are inherently difficult to parallelize in this way. In the literature, there have been attempts to parallelize algorithms such as Single-Linkage, which in principle can also be extended to the broader scope of hierarchical density-based clustering. 

In the paper titled as "Hierarchical Density-Based Clustering using MapReduce" and accepted for publication at the Transactions on Big Data (IEEE) journal (follow the preprint paper link: https://ieeexplore.ieee.org/document/8674542), we proposed an exact MapReduce version of the state-of-the-art hierarchical density-based clustering algorithm HDBSCAN* based on the Random Blocks approach. Also, we discussed why adapting previous approaches to parallelize Single-Linkage clustering using MapReduce leads to very inefficient solutions when one wants to compute density-based clustering hierarchies. To be able to apply hierarchical density-based clustering to large data sets using MapReduce, we proposed in this paper a different parallelization scheme that efficiently computes an approximate clustering hierarchy based on a recursive sampling scheme. The scheme is based on HDBSCAN*, combined with a data summarization technique called data bubbles. The method was evaluated in terms of both runtime and quality of the approximation on a number of datasets, showing its effectiveness and scalability.  

Current stable version of MR-HDBSCAN Star -> mr-hdbscanstar-project.v-0.1.zip


# Datasets

- https://archive.ics.uci.edu/ml/datasets/YearPredictionMSD#
- https://archive.ics.uci.edu/ml/datasets/Poker+Hand
- http://archive.ics.uci.edu/ml/datasets/gas+sensors+for+home+activity+monitoring
- https://archive.ics.uci.edu/ml/datasets/Skin+Segmentation
- https://archive.ics.uci.edu/ml/datasets/HEPMASS
- https://archive.ics.uci.edu/ml/datasets/HIGGS
- https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
