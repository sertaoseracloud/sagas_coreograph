import { EventData, EventHubProducerClient } from "@azure/event-hubs";
import { AzureFunction, Context, HttpRequest } from "@azure/functions"

const connectionString = process.env.AZURE_EVENTHUB_CONNECTIONSTRING
const eventHubname = process.env.AZURE_EVENTHUB_NAME

const producerClient = new EventHubProducerClient(
    connectionString,
    eventHubname
);

const httpTrigger: AzureFunction = async function (context: Context, req: HttpRequest): Promise<void> {
    const { fromAccount, toAccount, amount } = req.body;

    // Check if the required fields are present in the request body
    if (!fromAccount || !toAccount || !amount) {
        context.log.error("ClientFunction: Missing required properties in message:", req.body);
        context.res = {
            status: 400,
            body: "Faltando propriedades obrigatórias na requisição.",
        };
        return;
    }

    // Prepare the event data
    const event: EventData = {
        body: { type: "StartTransfer", fromAccount, toAccount, amount },  
    };

    try {
        // Publish the event to the Event Hub
        await producerClient.sendBatch([event]);

        // Respond with a success status
        context.res = {
            status: 202,
            body: "Transação iniciada.",
        };
    } catch (error) {
        context.log.error("Failed to send event to Event Hub:", error);
        context.res = {
            status: 500,
            body: "Erro ao iniciar a transação.",
        };
    }
};

export default httpTrigger;