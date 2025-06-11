const tsToString = (timestamp) => `${new Date(timestamp).toISOString()} (${timestamp})`;

export default tsToString;
