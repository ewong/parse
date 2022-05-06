# Parse

This readme is broken into several sections. The section summaries are below.

1. Assumptions - Assumptions made when creating the code.
2. Architecture - Description of the parts of the app.
3. Tests - Description of the tests that were applied to the code.
4. Closing remarks - Last words 

# Assumptions

1. The csv is the only source of truth. 
- We should only apply changes to the account once all the data has been verified and successfully calculated. 
- If anything fails, we should rollback and not apply changes. Otherwise, we corrupt the official 

2. Withdrawal chargebacks are treated as ATM withdrawal chargebacks. 
- No action is taken during dispute and resolve, as the situation is being investigated.
- If the dispute is valid, the chargeback will be applied as a credit to the available balance & total.

3. All CSV files names are unique.
- They are used to create the temp directory for current account balance calculations

4. The CSV files are fed into the app sequentially, since each line in a file is said to be in chronological order.

# Architecture

The app is broken up into two stages:
1. Current balance calculation by client.
2. Updating the official balance from step one + outputting all balances.

Each stage has the same architecture. It's modelled after a load balancer structure. The diagram of the structure is shown below:

                                                   worker0
                                                  /    :
            (data clustering) --> (load balancer) -    :
                                                  \    :
                                                   workerN



# Tests

# Closing remarks