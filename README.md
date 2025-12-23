# Critical Vitals Alert System

This project simulates generating vital signs for multiple patients, detects when those vitals fall below critical thresholds, logs these occurrences, and sends alert messages to a Kafka topic.  It also ensures the system continues generating normal vital signs and keeps track of vital signs on a per-patient basis.

## Usage

1.  **Clone the repository:**

    ```bash
    git clone https://github.com/Gautam0610/critical-vitals-alert.git
    cd critical-vitals-alert
    ```

2.  **Build the Docker image:**

    ```bash
    docker build -t critical-vitals-alert .
    ```

3.  **Run the Docker container:**

    ```bash
    docker run -d -p 8080:8080 critical-vitals-alert
    ```

## Configuration

*   **Kafka Configuration:**  Ensure your Kafka broker is running and accessible.  The application uses the `kafka-python` library.  Update the Kafka configuration in `main.py` as needed.
*   **Critical Thresholds:**  The critical thresholds for vital signs are defined in `main.py`.  Modify these values as required.

## Dependencies

*   Python 3.9
*   kafka-python
*   Faker

## License

[Specify the license, e.g., MIT]