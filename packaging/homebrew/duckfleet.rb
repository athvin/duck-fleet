class Duckfleet < Formula
  desc "Native command-line client for DuckFleet"
  homepage "https://github.com/athvin/duck-fleet"
  version "@VERSION@"
  license "Apache-2.0"

  on_macos do
    on_arm do
      url "https://github.com/athvin/duck-fleet/releases/download/duckfleet-v@VERSION@/duckfleet-@VERSION@-aarch64-apple-darwin.tar.gz"
      sha256 "@MACOS_ARM_SHA256@"
    end
  end

  on_linux do
    on_intel do
      url "https://github.com/athvin/duck-fleet/releases/download/duckfleet-v@VERSION@/duckfleet-@VERSION@-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "@LINUX_X86_SHA256@"
    end
    on_arm do
      url "https://github.com/athvin/duck-fleet/releases/download/duckfleet-v@VERSION@/duckfleet-@VERSION@-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "@LINUX_ARM_SHA256@"
    end
  end

  def install
    bin.install "duckfleet"
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/duckfleet version")
  end
end
