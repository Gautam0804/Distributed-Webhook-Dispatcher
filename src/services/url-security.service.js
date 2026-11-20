
import dns from "node:dns/promises";
import ipaddr from "ipaddr.js";

const ALLOWED_PROTOCOLS = new Set(["http:", "https:"]);

const BLOCKED_HOSTNAMES = new Set([
  "localhost",
  "metadata.google.internal",
  "metadata",
]);

// Custom error for unsafe webhook destinations.
export class UnsafeDestinationError extends Error {
  constructor(message) {
    super(message);
    this.name = "UnsafeDestinationError";
  }
}

function isPublicIp(address) {
  try {
    const parsedAddress = ipaddr.process(address);
    return parsedAddress.range() === "unicast";
  } catch {
    return false;
  }
}

function validateUrlFormat(destinationUrl) {
  let url;

  try {
    url = new URL(destinationUrl);
  } catch {
    throw new UnsafeDestinationError("Invalid destination URL");
  }

  if (!ALLOWED_PROTOCOLS.has(url.protocol)) {
    throw new UnsafeDestinationError(
      "Only HTTP and HTTPS destinations are allowed"
    );
  }

  if (url.username || url.password) {
    throw new UnsafeDestinationError(
      "URLs containing credentials are not allowed"
    );
  }

  const hostname = url.hostname.toLowerCase();

  if (
    BLOCKED_HOSTNAMES.has(hostname) ||
    hostname.endsWith(".localhost") ||
    hostname.endsWith(".local") ||
    hostname.endsWith(".internal")
  ) {
    throw new UnsafeDestinationError(
      "Private or reserved hostnames are not allowed"
    );
  }

  return { url, hostname };
}

export async function resolveSafeDestination(destinationUrl) {
  const { url, hostname } = validateUrlFormat(destinationUrl);

  // Remove brackets around IPv6 literals for IP parsing.
  const normalizedHostname = hostname.replace(/^\[|\]$/g, "");

  let addresses;

  if (ipaddr.isValid(normalizedHostname)) {
    const parsed = ipaddr.parse(normalizedHostname);

    addresses = [
      {
        address: normalizedHostname,
        family: parsed.kind() === "ipv6" ? 6 : 4,
      },
    ];
  } else {
    try {
      addresses = await dns.lookup(normalizedHostname, {
        all: true,
        verbatim: true,
      });
    } catch {
      throw new UnsafeDestinationError(
        "Destination hostname could not be resolved"
      );
    }
  }

  if (addresses.length === 0) {
    throw new UnsafeDestinationError(
      "Destination hostname has no IP addresses"
    );
  }

  // Reject if ANY DNS result is private or reserved.
  for (const entry of addresses) {
    if (!isPublicIp(entry.address)) {
      throw new UnsafeDestinationError(
        "Destination resolves to a private or reserved IP address"
      );
    }
  }

  // Pin the request to the validated address.
  const selectedAddress = addresses[0];

  return {
    url,

    lookup: (_hostname, options, callback) => {
      if (options?.all) {
        callback(null, [
          {
            address: selectedAddress.address,
            family: selectedAddress.family,
          },
        ]);
        return;
      }

      callback(
        null,
        selectedAddress.address,
        selectedAddress.family
      );
    },
  };
}

export async function validateDestinationUrl(destinationUrl) {
  await resolveSafeDestination(destinationUrl);
}
