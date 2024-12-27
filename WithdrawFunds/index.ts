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
                context.log.error("WithdrawFunds: Invalid or undefined message body:", message);
                continue;
            }

            let parsedMessage;
            try {
                parsedMessage = JSON.parse(message);
            } catch (err) {
                context.log.error("WithdrawFunds: Error parsing message:", err);
                continue; // Skip to next message if parsing fails
            }

            const { fromAccount, amount, type } = parsedMessage;

            if(type !== "StartTransfer") {
                continue; // Continue processing other messages
            }

            if (!fromAccount || !amount) {
                context.log.error("WithdrawFunds: Missing required properties in message:", message);
                return;
            }

            context.log(`WithdrawFunds: Initiating withdrawal of ${amount} from ${fromAccount}`);

            // Simular lógica de saque
            const success = Math.random() > 0.2; // Simulação: 80% sucesso

            try {
                if (success) {
                    const event: EventData = {
                        body:{ type: "WithdrawCompleted", fromAccount, amount }  
                    };
                    await producerClient.sendBatch([event]);
                    context.log("WithdrawFunds: Withdrawal succeeded.");
                } else {
                    const event: EventData = {
                        body:{ type: "WithdrawCompensation", fromAccount, amount }  
                    };
                    await producerClient.sendBatch([event]);
                    context.log.error("WithdrawFunds: Withdrawal failed. Compensation event emitted.");
                }
            } catch (eventError) {
                context.log.error(
                    `WithdrawFunds: Error sending event for account ${fromAccount}:`,
                    eventError
                );
            }
        }
    } catch (error) {
        context.log.error("WithdrawFunds: Error processing messages:", error);
    } finally {
        await producerClient.close();
    }
};

export default eventHubTrigger;
