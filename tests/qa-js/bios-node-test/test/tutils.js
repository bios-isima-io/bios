const BIOS_ENDPOINT = process.env.BIOS_ENDPOINT_JS
  || process.env.TFOS_ENDPOINT
  || 'https://localhost';

// MailHog API endpoint
const smtpHost = process.env.BIOS_TEST_SMTP || 'localhost';
const SMTP_ENDPOINT = `http://${smtpHost}:8025`;

function createRandomString(length) {
  // const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  const chars = 'abcdefghijklmnopqrstuvwxyz';
  let result = "";
  for (let i = 0; i < length; i++) {
    result += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return result;
}

export {
  BIOS_ENDPOINT,
  createRandomString,
  SMTP_ENDPOINT,
}
