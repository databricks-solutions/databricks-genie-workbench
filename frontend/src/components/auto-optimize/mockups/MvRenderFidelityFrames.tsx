/**
 * M3 fidelity frames — unservable IQ-scan empty (MV-D113 d4).
 *
 * Renders the REAL MvAdvisoryEmpty so the fidelity gate compares the shipped
 * fourth empty variant against frame 7b's layout density, not a hand-drawn copy.
 */
import { AdvisorySection } from "./MvIqScanAdvisoryMockups"
import { MvAdvisoryEmpty } from "@/components/auto-optimize/MvIqScanAdvisorySection"

export function IqScanUnservableFrame() {
  return (
    <AdvisorySection>
      <MvAdvisoryEmpty skipReason="NO_SERVABLE_MEASURES" measuresFound={3} />
    </AdvisorySection>
  )
}
