package io.papermc.paper.worldline;

/** A one-shot receipt token: a rejected first movement must not be replaced by a later success. */
public final class WorldlineFirstReplayMovement {
    private boolean awaiting;

    public void arm() {
        this.awaiting = true;
    }

    public boolean beginMovement() {
        final boolean first = this.awaiting;
        this.awaiting = false;
        return first;
    }
}
