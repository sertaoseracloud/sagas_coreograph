import { EventData, EventHubProducerClient } from "@azure/event-hubs";
import { AzureFunction, Context } from "@azure/functions";

const connectionString = process.env.AZURE_EVENTHUB_CONNECTIONSTRING;
const eventHubname = process.env.AZURE_EVENTHUB_NAME;

const eventHubTrigger: AzureFunction = async function (
    context: Context,
    eventHubMessages: any[]
): Promise<void> {
    const producerClient = new EventHubProducerClient(
        connectionString,
        eventHubname
    );
    
    try {
        for (const message of eventHubMessages) {
            if (!message) {
                context.log.error("DepositFunds: Invalid or undefined message body:", message);
                continue;
            }

            let parsedMessage;
            try {
                parsedMessage = JSON.parse(message);
            } catch (err) {
                context.log.error("DepositFunds: Error parsing message:", err);
                continue; // Skip to next message if parsing fails
            }

            const { toAccount, amount, type } = parsedMessage;

            if(type !== "StartTransfer") {
                continue; // Continue processing other messages
            }

            if (!toAccount || !amount) {
                context.log.error("DepositFunds: Missing required properties in message:", parsedMessage);
                continue; // Continue processing other messages
            }

            context.log(`DepositFunds: Initiating deposit of ${amount} to ${toAccount}`);

            // Simulate deposit logic
            const success = Math.random() > 0.2; // Simulate 80% success

            try {
                if (success) {
                    const event: EventData = {
                        body:{ type: "DepositCompleted", toAccount, amount }  
                    };
                    await producerClient.sendBatch([event]);
                    context.log("DepositFunds: Deposit succeeded.");
                } else {
                    const event: EventData = {
                        body:{ type: "DepositCompensation", toAccount, amount }  
                    };
                    await producerClient.sendBatch([event]);
                    context.log.error("DepositFunds: Deposit failed. Compensation event emitted.");
                }
            } catch (eventError) {
                context.log.error(
                    `DepositFunds: Error sending event for account ${toAccount}:`,
                    eventError
                );
            }
        }
    } catch (error) {
        context.log.error("DepositFunds: Error processing messages:", error);
    } finally {
        await producerClient.close();
    }
};

export default eventHubTrigger;
