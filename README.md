# im_not_a_rapper
Personal helper code for working with the UK Biobank RAP. This might save you a TON of time for setting up your project environment, reducing the fixed costs associated with working on the UK Biobank RAP. 

# First Time Setup on the RAP
## Launching an Instance in a Project
- Log in here DNAnexus - Log In
- If you aren’t using an existing project, create a New Project
- Dispense data to it. This will allow you to access the UK Biobank’s data.
- Next, if you want to run code, go to Tools → JupyterLab
- Then, either relaunch an old instance (e.g., the “62 GB RAM…” one shown in mine here) or create a new one with “New JupyterLab” on the top right.
- Set the priority to high, choose the Instance Type (based on the computational resources and costs here), the duration, and optionally name it. I recommend naming it something based on the computational resources. You’ll likely have some setups that will be useful such as for a large amount of RAM, another that is cheaper with smaller compute, etc.

It could take 2-15 minutes to start up. Then, open it, and you can will probably want to create either a Terminal and/or a Notebook.

## Creating an Environment: Code, Files, and Packages
This will save you a ton of time and pain if you can set this up. By default, the UK Biobank RAP will delete all your files, your code, your data, and your environments, unless you do something like this. Once this is set up, you just need to run a file for 1-2 minutes and your environment (code, packages, etc.) will be set up. 

#### Create a conda environment you can reload later (one time, unless you update it)
Create a .yaml file containing the packages you need in your environment. Here is an example. 
```
name: my_env_name
channels:
  - conda-forge
  - bioconda
  - defaults
dependencies:
  - python=3.10
  - pip
  - plink2
  - bash
  - numpy
  - pandas
  - dask
  - scipy
  - matplotlib
  - seaborn
  - statsmodels
  - jupyterlab
  - plotly
  - ipykernel
  - tqdm
```
I recommend storing it in your GitHub repository, so it can be downloaded into the RAP each time.

Set up GitHub (one time)
Create ssh keys on the RAP.
```ssh-keygen -t ed25519 -C "my.email@email.com"```

Save them to some folder. 

#### Copy the keys 
```
cat my_folder/my_ssh_keys.sh
```

Go to your GitHub profile online and then paste the ssh keys

#### Save your ssh keys to permanent storage. 
```
dx mkdir /ssh_keys/
dx upload my_folder/my_ssh_keys.sh –path /ssh_keys/
```

#### Load your conda environment and GitHub repositories (every time)
This is set up for two repositories. You can add or remove the number of repositories based on how many repositories you define and call. The highlighted areas are the variables you would need to change (e.g., your GitHub repository names) and code you may want to modify (e.g., if you want to add another repository).

#### Edit the code below. Then, create a file and paste this code into it. 
Edit the lines with "# ***" comments to ensure your variable names match with your repo and file paths.
```
#!/usr/bin/env bash
set +e +u +o pipefail

# SSH key variables
HOME_SSH="$HOME/.ssh"       # Where you will download the ssh keys to (ephemeral storage)
DX_KEY_DIR="/ssh_keys/"   # Folder where you will "dx upload" and "dx download" ssh keys to # *** 

# GitHub repo names
MY_FIRST_REPO_SSH="${MY_FIRST_REPO_SSH:-git@github.com:MyUserName/my_repo.git}"           # 1st repo 
MY_SECOND_REPO_SSH="${MY_SECOND_REPO_SSH:-git@github.com:MyUserName/my_other_repo.git}" # 2nd repo  # *** 

# GitHub branch names you are working on (currently assumes each repo has the same branch name)
**BRANCH="${BRANCH:-my_branch_name}" # Branch of the repos you want to be on    
**
# Local locations to download/clone the GitHub repositories to
LOCAL_MY_FIRST_REPO="$HOME/my_repo"               # Local copy of the first repo
LOCAL_MY_SECOND_REPO="$HOME/sibreg/my_other_repo" # Local copy of the other repo  # *** 

# Your GitHub user name and email
GITHUB_USERNAME="MyUserName"              # *** 
GITHUB_EMAIL="my.email@email.com"     # *** 

# Conda environment variables
ENV_NAME="my_env_name"  # Name of your conda environment. See the {my_env_name}.tar.gz name   # *** 
ENV_TARBALL_FILE="${ENV_NAME}.tar.gz"
ENV_TARBALL_PATH="/mnt/project/conda_envs/${ENV_TARBALL_FILE}" # Compressed conda env location   # *** 
LOCAL_ENV_PATH="$HOME/conda_envs/${ENV_NAME}" # Save the conda env here  # *** 
ENV_YAML_PATH="${LOCAL_MY_FIRST_REPO}/misc/${ENV_NAME}.yaml"  # Wherever your .yaml file is, recommended to be in your GitHub repository  # *** 
log(){ echo "[$(date +'%F %T')] $*"; } 

# Ensure local ~/.ssh and DX folders exist
init_ssh_dirs() {
  mkdir -p "$HOME_SSH"; chmod 700 "$HOME_SSH"
  dx ls /ssh_keys     >/dev/null 2>&1 || dx mkdir /ssh_keys
  dx ls "$DX_KEY_DIR" >/dev/null 2>&1 || dx mkdir "$DX_KEY_DIR"
}

# Download from DX if present; else generate locally and upload to DX
ensure_ssh_keys() {
  init_ssh_dirs

  # Check if ssh keys are stored locally
  if [[ -s "$HOME_SSH/id_ed25519" ]]; then
    log "Local SSH keys already exist at $HOME_SSH/id_ed25519"
    return 0
  fi

  # Check if ssh keys are in the "dx download" location
  if dx ls "${DX_KEY_DIR}/id_ed25519" >/dev/null 2>&1; then
    log "Found alias keys in $DX_KEY_DIR; downloading to $HOME_SSH…"
    dx download -f "${DX_KEY_DIR}/id_ed25519"     -o "$HOME_SSH/id_ed25519"
    dx download -f "${DX_KEY_DIR}/id_ed25519.pub" -o "$HOME_SSH/id_ed25519.pub" 2>/dev/null || true
  else
    # Fallback: newest timestamped objects
    priv="$(dx find data --path "$DX_KEY_DIR" --name 'id_ed25519*'     --brief --latest 2>/dev/null || true)"
    pub="$( dx find data --path "$DX_KEY_DIR" --name 'id_ed25519.pub*' --brief --latest 2>/dev/null || true)"
    if [[ -n "$priv" ]]; then
      log "Found timestamped keys in $DX_KEY_DIR; downloading latest to $HOME_SSH…"
      dx download -f "$priv" -o "$HOME_SSH/id_ed25519"
      [[ -n "$pub" ]] && dx download -f "$pub" -o "$HOME_SSH/id_ed25519.pub" || true
    else
      # Nothing in DX: create locally and upload both timestamped + alias
      log "No keys in $DX_KEY_DIR; generating a new ed25519 keypair locally…"
      ssh-keygen -t ed25519 -C "rap-$(whoami)@$(hostname)" -f "$HOME_SSH/id_ed25519" -N ""
      ts="$(date +'%Y%m%d-%H%M%S')"
      log "Uploading new keys to $DX_KEY_DIR (timestamped + alias)…"
      dx upload "$HOME_SSH/id_ed25519"     --path "${DX_KEY_DIR}/id_ed25519.${ts}"
      dx upload "$HOME_SSH/id_ed25519.pub" --path "${DX_KEY_DIR}/id_ed25519.pub.${ts}"
      dx upload "$HOME_SSH/id_ed25519"     --path "${DX_KEY_DIR}/id_ed25519"
      dx upload "$HOME_SSH/id_ed25519.pub" --path "${DX_KEY_DIR}/id_ed25519.pub" || true
    fi
  fi

  # Permissions for OpenSSH
  chmod 600 "$HOME_SSH/id_ed25519" 2>/dev/null || true
  [[ -f "$HOME_SSH/id_ed25519.pub" ]] && chmod 644 "$HOME_SSH/id_ed25519.pub"
}

# Start agent and add the key (each session)
start_agent_and_add() {
  [[ -n "${SSH_AUTH_SOCK:-}" ]] || eval "$(ssh-agent -s)" >/dev/null
  ssh-add -l >/dev/null 2>&1 || ssh-add "$HOME_SSH/id_ed25519" >/dev/null
}

# Wrapper keeps your existing call site the same
setup_ssh() {
  ensure_ssh_keys
  # known_hosts (idempotent)
  touch "$HOME_SSH/known_hosts"
  ssh-keyscan -t rsa,ecdsa,ed25519 github.com 2>/dev/null | sort -u \
    | cat - "$HOME_SSH/known_hosts" | awk '!seen[$0]++' > "$HOME_SSH/known_hosts.tmp"
  mv "$HOME_SSH/known_hosts.tmp" "$HOME_SSH/known_hosts"
  chmod 644 "$HOME_SSH/known_hosts"

  # Minimal SSH config (idempotent)
  if ! grep -qE '^Host[[:space:]]+github.com' "$HOME_SSH/config" 2>/dev/null; then
    cat >> "$HOME_SSH/config" <<'CFG'
Host github.com
  HostName github.com
  User git
  IdentityFile ~/.ssh/id_ed25519
  IdentitiesOnly yes
  StrictHostKeyChecking accept-new
CFG
    chmod 600 "$HOME_SSH/config"
  fi

  start_agent_and_add
}


# Clone if missing; otherwise ensure correct remote
git_get_repo() {
  local dir="$1" url="$2"
  mkdir -p "$(dirname "$dir")"
  if [[ ! -d "$dir/.git" ]]; then
    log "[clone] $dir ← $url"
    git clone "$url" "$dir"
  else
    log "[exists] $dir"
    (
      cd "$dir"
      # If remote URL mismatched, set it
      current_url="$(git remote get-url origin 2>/dev/null || echo "")"
      if [[ -n "$current_url" && "$current_url" != "$url" ]]; then
        log "[fix-remote] $dir origin → $url"
        git remote set-url origin "$url"
      fi
    )
  fi
}

git_switch_branch () {
  local dir="$1" branch="$2"
  (
    set -euo pipefail
    cd "$dir"

    # If there's no 'origin' remote, just make/switch the branch locally.
    if ! git remote get-url origin >/dev/null 2>&1; then
      if git show-ref --verify --quiet "refs/heads/$branch"; then
        git switch "$branch"
      else
        git switch -c "$branch"
      fi
      exit 0
    fi

    # Sync remotes/tags and prune stale refs
    git fetch --all --tags --prune

    if git show-ref --verify --quiet "refs/heads/$branch"; then
      # Branch exists locally
      git switch "$branch"
    else
      # Branch not local — does it exist on origin?
      if git ls-remote --exit-code --heads origin "$branch" >/dev/null 2>&1; then
        # Create local branch tracking origin/branch
        git switch -c "$branch" --track "origin/$branch"
      else
        # Create brand-new branch and publish it
        git switch -c "$branch"
        git push -u origin "$branch"
      fi
    fi

    # Ensure upstream is set (in case the earlier steps didn't)
    git rev-parse --abbrev-ref --symbolic-full-name @{u} >/dev/null 2>&1 \
      || git branch --set-upstream-to="origin/$branch" "$branch" || true

    # Update branch: fast-forward only; don't rebase or create merge commits
    git -c pull.ff=only pull || true
  )

}

##############################
### Conda environment code ###
##############################
create_conda_env_tar(){
  if [ ! -f "${ENV_TARBALL_PATH}" ]; then
    log "Packed environment '${ENV_TARBALL_PATH}' not found. Building it now..."
    log "(This is a one-time setup and will be slow.)"

    # A. Create the environment from the YAML file
    log "Creating Conda environment from YAML..."
    conda env create -f "${ENV_YAML_PATH}"

    # B. Pack the newly created environment into a tarball
    log "Packing '${ENV_NAME}' environment locally..."
    if ! command -v conda-pack >/dev/null 2>&1; then
      conda install -y -n base -c conda-forge conda-pack
    fi
    conda pack -n "${ENV_NAME}" -o "${ENV_TARBALL_FILE}"

    # C. Upload the new tarball to persistent storage for future runs
    log "dx uploading locally-packed environment to persistent storage..."
    dx upload "${ENV_TARBALL_FILE}" "${ENV_TARBALL_PATH}"

    log "✅ Environment packed and uploaded successfully."
  else
    log "✅ Pre-built environment '${ENV_TARBALL_PATH}' found in persistent storage."
  fi
}

activate_conda_env(){
  log(){ echo "[$(date +'%F %T')] $*"; }

  # Extract environment or check whether the environment has been extracted
  if [[ -f "${LOCAL_ENV_PATH}/bin/activate" ]]; then
    log "✅ Conda environment already unpacked at ${LOCAL_ENV_PATH} — skipping extraction."
  else
    log "Extracting pre-built Conda environment..."
    mkdir -p "${LOCAL_ENV_PATH}"
    tar -xf "${ENV_TARBALL_PATH}" -C "${LOCAL_ENV_PATH}"
  fi 	

  # Activate Conda environment
  log "Activating Conda environment..."
  set +u
  source "${LOCAL_ENV_PATH}/bin/activate"
  set -u
  conda-unpack

  log "🚀  All setup complete. Environment 'ds_env' is active."
}


main() {
  cd "$HOME"

  # If not running under bash, re-exec under bash login shell
  if [ -z "${BASH_VERSION:-}" ]; then
    exec bash --login "$0" "$@"
  fi

  ### SSH keys for GitHub ###
  setup_ssh

  ### GitHub ###
  # Quick auth check (exit code 1 with "successfully authenticated" text is normal)
  if ! ssh -T git@github.com 2>&1 | tee /tmp/gh_ssh_test.txt | grep -qi "successfully authenticated"; then
    if grep -q "Permission denied (publickey)" /tmp/gh_ssh_test.txt; then
      echo
      echo "ERROR: Your public key is not on your GitHub account. Add this key:"
      echo "-----8<-----"
      cat "$HOME_SSH/id_ed25519.pub"
      echo "-----8<-----"
      echo "GitHub -> Settings -> SSH and GPG keys -> New SSH key, then re-run this script."
      exit 1
    fi
  fi
  
  ssh-keyscan -H github.com >> ~/.ssh/known_hosts 2>/dev/null

  # 1a) Download the first GitHub repository (clone)          
  git_get_repo "$LOCAL_MY_FIRST_REPO" "$MY_FIRST_REPO_SSH"   
  # 2a) Switch to the branch in the first repo (checkout)     
  git_switch_branch  "$LOCAL_MY_FIRST_REPO" "$BRANCH"         # *** Optional second repo
  log "Ready on branch '$BRANCH' in:"
  log "  $LOCAL_MY_FIRST_REPO"

  # 1b) Download the second GitHub repository (clone)        
  git_get_repo "$LOCAL_MY_SECOND_REPO" "$MY_SECOND_REPO_SSH"  
  # 2b) Switch to the branch in the second repo (checkout)
  git_switch_branch  "$LOCAL_MY_SECOND_REPO" "$BRANCH"       # *** Optional second repo
  log "Ready on branch '$BRANCH' in:"
  log "  $LOCAL_MY_FIRST_REPO"

  git config --global user.name "$GITHUB_USERNAME"
  git config --global user.email "$GITHUB_EMAIL"

  ### Conda ###
  create_conda_env_tar
  activate_conda_env
  log "🚀 All setup complete. Your environment is ready."

}

main
```

#### Save the file
```
dx upload my_startup_file.sh
```

Now that all of this has been set up, you won’t need to do that again! Instead, you will just do this every time to get all the GitHub files and packages you need, downloading and then running the startup file:
```
dx download my_startup_file.sh
. my_startup_file.sh
```

### A Final Note on First Time Setup
Code: When you edit your code, make sure you upload it to GitHub. This is much easier than “dx upload”ing every time with every file. 

Data: When you create files you want to save, make sure you “dx upload” them. You may want to add “dx upload your_file_name” to the end of your scripts.

Editing Code: You may prefer to edit your code in an IDE like VSCode rather than vim in the terminal. The GitHub setup here makes this very straightforward: edit your code in your preferred IDE, upload it to GitHub, then run the startup code here which will grab all the latest GitHub code. 

