# buzzline-03-monsuru  
Streaming data does not have to be simple text. Many of us are familiar with streaming video content and audio (e.g., music) files.  

In this project, we’ll work with two different types of data: **JSON** and **CSV**. Each type of data will be processed using custom Kafka producers and consumers.  

---

## **Custom JSON Kafka Producer and Consumer**  

### **JSON Producer**  
The JSON producer (`json_producer_prince.py`) sends custom JSON messages to a Kafka topic. Each message contains a `message` and `author` field.  

#### Running the JSON Producer:  
1. Navigate to the `producers` folder.  
2. Activate your virtual environment:  
   - **Windows:**  
     ```bash
     .venv\Scripts\activate
     ```  
   - **Mac/Linux:**  
     ```bash
     source .venv/bin/activate
     ```  
3. Run the JSON producer script:  
   - **Windows:**  
     ```bash
     py -m producers.json_producer_prince
     ```  
   - **Mac/Linux:**  
     ```bash
     python3 -m producers.json_producer_prince
     ```  

---

### **JSON Consumer**  
The JSON consumer (`json_consumer_prince.py`) reads messages from the Kafka topic in real time and performs basic analytics.  

#### Running the JSON Consumer:  
1. Navigate to the `consumers` folder.  
2. Activate your virtual environment (same as above).  
3. Run the JSON consumer script:  
   - **Windows:**  
     ```bash
     py -m consumers.json_consumer_prince
     ```  
   - **Mac/Linux:**  
     ```bash
     python3 -m consumers.json_consumer_prince
     ```  

#### Real-Time Analytics:  
The consumer logs each message and processes the message and author fields. It performs basic analytics, such as counting messages or identifying trends in the data. 

---

## **Custom CSV Kafka Producer and Consumer**  

### **CSV Producer**  
The CSV producer (`csv_producer_prince.py`) reads health data from a CSV file and sends it to a Kafka topic. The CSV file contains the following columns:  
- `steps`: Number of steps taken.  
- `heart_rate`: Heart rate in beats per minute.  
- `calories_burned`: Calories burned during the activity.  
- `sleep_hours`: Hours of sleep.  
- `hydration_liters`: Liters of water consumed.  

Each row of the CSV file is converted into a JSON message with an additional `timestamp` field.  

#### Running the CSV Producer:  
1. Navigate to the `producers` folder.  
2. Activate your virtual environment (same as above).  
3. Run the CSV producer script:  
   - **Windows:**  
     ```bash
     py -m producers.csv_producer_prince
     ```  
   - **Mac/Linux:**  
     ```bash
     python3 -m producers.csv_producer_prince
     ```  

---

### **CSV Consumer**  
The CSV consumer (`csv_consumer_prince.py`) reads messages from the Kafka topic and performs real-time analytics on the health data.  

#### Running the CSV Consumer:  
1. Navigate to the `consumers` folder.  
2. Activate your virtual environment (same as above).  
3. Run the CSV consumer script:  
   - **Windows:**  
     ```bash
     py -m consumers.csv_consumer_prince
     ```  
   - **Mac/Linux:**  
     ```bash
     python3 -m consumers.csv_consumer_prince
     ```  

#### Real-Time Analytics:  
The consumer maintains a rolling window of the last 5 readings of `calories_burned`. It calculates the range of values in the window and detects a **stall** if the range is less than or equal to 0.2.  

- **Stall Detection:**  
  If a stall is detected, the consumer logs an alert, indicating that the calories burned have remained stable over the last 5 readings.  

---

## **How to Run the Project**  
1. Start Zookeeper and Kafka (see [SETUP-KAFKA.md](SETUP-KAFKA.md)).  
2. Activate your virtual environment:  
   - **Windows:**  
     ```bash
     .venv\Scripts\activate
     ```  
   - **Mac/Linux:**  
     ```bash
     source .venv/bin/activate
     ```  
3. Run the producers and consumers in separate terminals:  
   - Start the JSON producer and consumer.  
   - Start the CSV producer and consumer.  

---

## **About the Stock Market (JSON Example)**  
The stock market is dynamic, with prices and volumes changing rapidly. The JSON producer simulates real-time stock market data, generating updates every few seconds with key details like stock price, trading volume, and percentage change.  

The consumer processes these messages, analyzing the last 5 updates for each stock to detect significant events such as price spikes, unusual volumes, or emerging trends. By focusing on recent data and filtering out minor fluctuations, the system provides meaningful insights for traders and analysts.  

---

## **About Health Monitoring (CSV Example)**  
This example uses CSV data to track fitness metrics like steps, heart rate, calories burned, sleep hours, and hydration. The producer streams updates every 15 seconds, converting each CSV row into a JSON message with a timestamp.  

The consumer analyzes the last 5 updates to detect trends or anomalies, such as stalled calorie burn or changes in hydration. By focusing on recent data, the system provides real-time insights for personal health and fitness monitoring.  

---

## **License**  
This project is licensed under the MIT License. You are encouraged to fork, copy, explore, and modify the code as you like. See the [LICENSE](LICENSE) file for more.  