from pyspark import SparkConf, SparkContext


def idWithPrice(line):
    fields = line.split(",")
    return fields[0], float(fields[2])

conf = SparkConf().setMaster("local").setAppName("customerSpending")
sc = SparkContext(conf=conf)

data = sc.textFile("customer-orders.csv")
customer_spending = data.map(idWithPrice).reduceByKey(lambda x,y: x+y)

results = customer_spending.sortByKey().collect()

for customer, spending in results:
    print("%2s: %.2f$ " %(customer, spending))

# To sort by amounts spend by the customer
customer_spending_reverse = customer_spending.map(lambda x: (x[1], x[0])).sortByKey()
results = customer_spending_reverse.collect()

for spending, customer in results:
    print("%2s: %.2f$ " %(customer, spending))


