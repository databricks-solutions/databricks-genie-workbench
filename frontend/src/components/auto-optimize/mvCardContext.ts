import { createContext } from "react"

/**
 * Provided by MvProposalCard around its actions slot. The accept flow inside it
 * reports the provenance of the create result it got back, so the card's detail
 * can withhold the GRANT from a caller who doesn't own the view (MV-D120).
 * Null outside a card.
 */
export const MvCardCreateResultContext = createContext<((provenance: string | null) => void) | null>(null)
