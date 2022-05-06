# Parse

This readme is broken into several sections. The section summaries are below. Thanks for taking the time to read this.

1. Assumptions - Assumptions made when creating the code.
2. Architecture - Description of the parts of the app.
3. Testing - Description of the tests that were applied to the code.

# Assumptions

1. The csv is the only source of truth. 
- We should only apply changes to the final result once all the data has been verified and successfully calculated. 
- If anything fails, we should rollback and not apply changes. Otherwise, we corrupt the official data.

2. Withdrawal chargebacks are treated as ATM withdrawal chargebacks. 
- No action is taken during dispute and resolve, as the situation is being investigated.
- If the dispute is valid, the chargeback will be applied as a credit to the available balance & total.

3. All CSV files names are unique.
- They are used to create the temp directory for current running calculations

4. The CSV files are fed into the app sequentially, since each line in a file is to be in chronological order.

# Architecture

The app is broken up into two stages:

1. Running calculations by person.
2. Updating the official data from step one + outputting all results in the system.

Each stage has the same architecture. It's modelled after a load balancer structure. The diagram of the structure is shown below:

                                                      [ Worker 0 ]
                                                      /    :
            [ Data clustering ] --> [ Load balancer ] -    :
                                                      \    :
                                                      [ Worker N ]

For stage 1, we do the following:

- We cluster the transaction data by person in the data clustering section.
- Each cluster is built from 1 million lines in the csv file.
- Each block is sent to the load balancer section.
- Inside the load balancer, there is a manager that is spawned in its own thread.
- The manager checks to see if a block of data in the cluster belong to a client is already assigned to a worker.
- If a worker is assigned to the client, then it will send the block of data to the worker to process.
- Otherwise, it will spread the assignments to other workers in a round-robin fashion.
- Each worker is spawned in its own thread and calculate the current balance data for the client based
- If the client exists, then it pulls the previous data and begins the calculation from that point.
- Otherwise, it creates a new account and starts calculating from a clean slate.
- All the current balances, are stored in a temporary area until all the calculations are successfully done.
- Once they are all done, we go to the next stage.

For stage 2, we update the accounts and show the balances in the system. This is done by the following:

- We get all the the temporarily calculated file paths in stage 1 and batch them into blocks in the data clustering section.
- We send each block to the load balancer section.
- The load balancer manager, that's sitting in its own thread, distributes the blocks to each worker.
- The workers update the system balance with the summary balance in stage 1.
- We repeat this same process to output the data of the system. However, we batch each file path in the system data folder.

# Tests

There were two considerations for testing: Correctness and scaling. 

1. Correctness
- The tests in this repo covers as many cases as possible. 
- They can be found in the tests folder, along with the associated csv files.

2. Scaling
- During development, a surrogate data set was used. It can be found here:
https://s3-api.us-geo.objectstorage.softlayer.net/cf-courses-data/CognitiveClass/ML0101ENv3/labs/moviedataset.zip
- Specifically, the ratings csv (620 M) file was used to benchmark the scaling and speed of the system.
- Instead of calculating the account balance, the average movie rating was calculated per movie.
- That calculation was later replaced with account balance calculation.