import { setPlatformBus, type PlatformBus } from '@/stores/live'

// The shell calls ./boot once the module is mounted (contracts/federation.md)
// with its BootContext; the deployer keeps the shared realtime bus so job
// events ride the shell's single platform stream instead of a connection of
// its own.
export default function boot(ctx: { live?: PlatformBus }): void {
  setPlatformBus(ctx.live)
}
