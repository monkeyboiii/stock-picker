#!/usr/bin/env node

/**
 * OpenAPI → TypeScript Code Generator
 *
 * Fetches OpenAPI specs from running services and generates TypeScript types
 */

const { exec } = require('child_process');
const { promisify } = require('util');
const fs = require('fs');
const path = require('path');

const execAsync = promisify(exec);

// Service configurations
const SERVICES = [
  { name: 'trading', port: 8000, url: 'http://localhost:8000/openapi.json' },
  { name: 'backtest', port: 8001, url: 'http://localhost:8001/openapi.json' },
  { name: 'auth', port: 8003, url: 'http://localhost:8003/openapi.json' },
  { name: 'notification', port: 8004, url: 'http://localhost:8004/openapi.json' },
  { name: 'calculation', port: 8005, url: 'http://localhost:8005/openapi.json' },
];

const GENERATED_DIR = path.join(__dirname, '../src/generated');

// Ensure generated directory exists
if (!fs.existsSync(GENERATED_DIR)) {
  fs.mkdirSync(GENERATED_DIR, { recursive: true });
}

async function generateTypes() {
  console.log('🚀 Starting OpenAPI → TypeScript code generation...\n');

  for (const service of SERVICES) {
    const outputPath = path.join(GENERATED_DIR, `${service.name}.ts`);

    try {
      console.log(`📡 Fetching ${service.name} API spec from ${service.url}...`);

      // Use openapi-typescript to generate types
      const command = `npx openapi-typescript ${service.url} -o ${outputPath}`;

      await execAsync(command);

      console.log(`✅ Generated types for ${service.name} → ${outputPath}\n`);
    } catch (error) {
      console.error(`❌ Failed to generate types for ${service.name}:`);
      console.error(`   Make sure the service is running on port ${service.port}`);
      console.error(`   Error: ${error.message}\n`);

      // Create placeholder file if generation fails
      const placeholder = `// ${service.name} API types (placeholder)
// Run the service on port ${service.port} and run 'pnpm codegen' to generate types

export interface paths {}
export interface components {}
`;
      fs.writeFileSync(outputPath, placeholder);
      console.log(`⚠️  Created placeholder for ${service.name}\n`);
    }
  }

  console.log('✨ Code generation complete!');
  console.log('\nNext steps:');
  console.log('  1. Start your backend services (docker-compose up)');
  console.log('  2. Run: pnpm codegen');
  console.log('  3. Import type-safe clients: import { tradingClient } from "@repo/api/trading"');
}

generateTypes().catch((error) => {
  console.error('Fatal error:', error);
  process.exit(1);
});
