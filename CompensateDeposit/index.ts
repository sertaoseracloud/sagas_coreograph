import { AzureFunction, Context } from "@azure/functions";

const eventHubTrigger: AzureFunction = async function (
    context: Context,
    eventHubMessages: any[]
): Promise<void> {
    try {
        for (const message of eventHubMessages) {
            if (!message) {
                context.log.error("CompensateDeposit: Invalid or undefined message body:", message);
                continue;
            }

            // The message is stringified and contains escape characters, so we need to parse it twice.
            let parsedMessage;
            try {
                // Second parse: to convert the inner message into an object
                parsedMessage = JSON.parse(message);
            } catch (err) {
                context.log.error("CompensateDeposit: Error parsing message:", err);
                continue; // Skip to next message if parsing fails
            }

            const { toAccount, amount, type } = parsedMessage;

            if(type !== "DepositCompensation") {
                continue; // Continue processing other messages
            }

            if (!toAccount || !amount) {
                context.log.error("CompensateDeposit: Missing required properties in message:", parsedMessage);
                continue; // Continue processing other messages
            }

            context.log(`CompensateDeposit: Reverting deposit of ${amount} from ${toAccount}`);
            }
            } catch (error) {
                context.log.error("CompensateDeposit: Error processing messages:", error);
            }
};

export default eventHubTrigger;
