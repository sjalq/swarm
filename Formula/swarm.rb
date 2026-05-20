class Swarm < Formula
  desc "Multi-agent CLI orchestrator for LLM coding assistants"
  homepage "https://github.com/sjalq/swarm"
  license "MIT OR Apache-2.0"
  version "0.1.0"

  on_macos do
    on_arm do
      url "https://github.com/sjalq/swarm/releases/download/v#{version}/swarm-#{version}-aarch64-apple-darwin.tar.gz"
      sha256 "PLACEHOLDER_AARCH64_DARWIN_SHA256"
    end

    on_intel do
      url "https://github.com/sjalq/swarm/releases/download/v#{version}/swarm-#{version}-x86_64-apple-darwin.tar.gz"
      sha256 "PLACEHOLDER_X86_64_DARWIN_SHA256"
    end
  end

  on_linux do
    on_arm do
      url "https://github.com/sjalq/swarm/releases/download/v#{version}/swarm-#{version}-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "PLACEHOLDER_AARCH64_LINUX_SHA256"
    end

    on_intel do
      url "https://github.com/sjalq/swarm/releases/download/v#{version}/swarm-#{version}-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "PLACEHOLDER_X86_64_LINUX_SHA256"
    end
  end

  def install
    bin.install "swarm"
  end

  test do
    assert_match "swarm", shell_output("#{bin}/swarm --help")
  end
end
