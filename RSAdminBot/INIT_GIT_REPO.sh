#!/bin/bash
# Quick script to initialize git repo for !pushrsbots
# Run this on the Ubuntu server: bash /path/to/INIT_GIT_REPO.sh

cd /home/rsadmin/bots/mirror-world

if [ ! -d ".git" ]; then
    echo "Initializing git repository..."
    git init
    git config user.name "RSAdminBot"
    git config user.email "rsadminbot@users.noreply.github.com"
    git remote add origin git@github.com:neo-rs/rsbots.git 2>/dev/null || git remote set-url origin git@github.com:neo-rs/rsbots.git
    git checkout -b main 2>/dev/null || git branch -M main 2>/dev/null || true
    echo "✅ Git repository initialized"
else
    echo "✅ Git repository already exists"
fi

echo "Current git status:"
git status --short | head -20

