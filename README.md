# Locality-Sensitive Hashing with Apache Spark

This project demonstrates the implementation of Locality-Sensitive Hashing (LSH) using Apache Spark. LSH is an algorithmic technique that hashes input items so that similar items map to the same "buckets" with high probability, facilitating efficient approximate nearest neighbor searches in high-dimensional spaces.

## Project Structure

- `HashGenerator.py`: Contains the implementation of the LSH algorithm, including the hash functions and related utilities.
- `Main.py`: Serves as the entry point for the application, orchestrating the data processing and applying the LSH algorithm.
- `apartments.tsv`: A sample dataset used for testing and demonstrating the LSH implementation.

## Getting Started

### Prerequisites

- [Apache Spark](https://spark.apache.org/downloads.html) installed and configured.
- [Python](https://www.python.org/downloads/) installed.

### Installation

1. **Clone the Repository**:

   ```bash
   git clone https://github.com/MarawanHassaan/Locality-sensitive-Hashing-Apache-Spark.git
   cd Locality-sensitive-Hashing-Apache-Spark
   ```

2. **Install Dependencies**:

   Ensure that all necessary Python packages are installed. You can use `pip` to install any required packages:

   ```bash
   pip install -r requirements.txt
   ```

   *(Note: The `requirements.txt` file should list all dependencies. If it's not present, manually install the required packages.)*

## Usage

1. **Prepare the Dataset**:

   Ensure that the `apartments.tsv` file is properly formatted and located in the project directory. This file should contain the data you wish to process using LSH.

2. **Run the Application**:

   Execute the `Main.py` script to start the LSH process:

   ```bash
   python Main.py
   ```

   This script will read the dataset, apply the LSH algorithm, and output the results.

## Understanding Locality-Sensitive Hashing (LSH)

Locality-Sensitive Hashing is a technique used to efficiently find approximate nearest neighbors in high-dimensional data. It works by hashing input items multiple times so that similar items are more likely to collide in the same buckets. This approach reduces the dimensionality of the data and allows for faster similarity searches.

For a more in-depth understanding, refer to the [Locality-Sensitive Hashing (LSH) for MLlib](https://github.com/yu-iskw/SPARK-5992-LSH-design-doc/blob/master/design-doc.md) design document.

## Acknowledgments

- [Apache Spark](https://spark.apache.org/) for providing a robust framework for big data processing.
- The open-source community for continuous contributions and support.
