import { createServiceStarter } from './service-start.mjs';

export { isDailyUseAutoStartHeld } from './service-start.mjs';
export const createDshServiceStarter = options => createServiceStarter({ ...options, harness: 'dsh' });
