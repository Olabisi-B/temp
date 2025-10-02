#!/bin/bash
set -euo pipefail

# Versions
SRA_VERSION="3.0.6"
KRAKEN_VERSION="2.1.3"
BRACKEN_VERSION="2.8"

TOOLS_DIR="tools"
DB_DIR="databases"

mkdir -p "$TOOLS_DIR" "$DB_DIR"
cd "$TOOLS_DIR"

# Detect OS
OS=$(uname)

# ---- Install SRA Toolkit ----
if [[ ! -d "sratoolkit" ]]; then
  echo "Installing SRA Toolkit $SRA_VERSION..."
  if [[ "$OS" == "Linux" ]]; then
    wget -q https://ftp-trace.ncbi.nlm.nih.gov/sra/sdk/$SRA_VERSION/sratoolkit.$SRA_VERSION-ubuntu64.tar.gz
    tar -xzf sratoolkit.$SRA_VERSION-ubuntu64.tar.gz
    mv sratoolkit.$SRA_VERSION-ubuntu64 sratoolkit
    rm sratoolkit.$SRA_VERSION-ubuntu64.tar.gz
  elif [[ "$OS" == "Darwin" ]]; then
    curl -sO https://ftp-trace.ncbi.nlm.nih.gov/sra/sdk/$SRA_VERSION/sratoolkit.$SRA_VERSION-mac64.tar.gz
    tar -xzf sratoolkit.$SRA_VERSION-mac64.tar.gz
    mv sratoolkit.$SRA_VERSION-mac64 sratoolkit
    rm sratoolkit.$SRA_VERSION-mac64.tar.gz
  else
    echo "Unsupported OS: $OS"
    exit 1
  fi
else
  echo "SRA Toolkit already installed."
fi

# ---- Install Kraken2 ----
if [[ ! -d "kraken2" ]]; then
  echo "Installing Kraken2 $KRAKEN_VERSION..."
  wget -q https://github.com/DerrickWood/kraken2/archive/refs/tags/v$KRAKEN_VERSION.tar.gz
  tar -xzf v$KRAKEN_VERSION.tar.gz
  mv kraken2-$KRAKEN_VERSION kraken2
  cd kraken2 && ./install_kraken2.sh . && cd ..
  rm v$KRAKEN_VERSION.tar.gz
else
  echo "Kraken2 already installed."
fi

# ---- Install Bracken ----
if [[ ! -d "bracken" ]]; then
  echo "Installing Bracken $BRACKEN_VERSION..."
  wget -q https://github.com/jenniferlu717/Bracken/archive/refs/tags/v$BRACKEN_VERSION.tar.gz
  tar -xzf v$BRACKEN_VERSION.tar.gz
  mv Bracken-$BRACKEN_VERSION bracken
  cd bracken && ./install_bracken.sh . && cd ..
  rm v$BRACKEN_VERSION.tar.gz
else
  echo "Bracken already installed."
fi

# ---- Update PATH ----
export PATH="$(pwd)/sratoolkit/bin:$(pwd)/kraken2:$(pwd)/bracken:$PATH"
echo "Updated PATH for this session."

# ---- Optional DB download ----
echo
read -p "Do you want to download the MiniKraken2 (8GB) database now? [y/N] " RESP
if [[ "$RESP" =~ ^[Yy]$ ]]; then
  cd ../"$DB_DIR"
  if [[ ! -d "minikraken2_v2_8GB_201904" ]]; then
    echo "Downloading MiniKraken2 database (~8 GB)..."
    wget https://genome-idx.s3.amazonaws.com/kraken/minikraken2_v2_8GB_201904_UPDATE.tgz
    tar -xzf minikraken2_v2_8GB_201904_UPDATE.tgz
    rm minikraken2_v2_8GB_201904_UPDATE.tgz
  else
    echo "MiniKraken2 DB already exists."
  fi
fi

echo
echo "Setup complete!"
echo "Tools installed in: $(realpath ../$TOOLS_DIR)"
echo "Databases (if downloaded) in: $(realpath ../$DB_DIR)"
