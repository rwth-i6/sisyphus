# tool with auto completion for a simple way to look though zipped work directories
# Requires zsh
# Install: add 'source path/to/this_file.zsh' into .zshrc
# v path/to/tar/file tar_internal_path

_v() {
  # v completion.
  # Needs documentation
  
  local _tar_cmd tf tmp tmpb del index
  
  # Now we complete...
  
  
  if (( CURRENT == 2 )); then
    arguments=( '*:files:_files' )
    _arguments -s $arguments
  else 
    tf=$words[2]
  
    if [[ $tf != $_tar_cache_name && -f $tf ]]; then
      _tar_cache_list=("${(@f)$(tar -tf $tf 2> /dev/null)}")
      _tar_cache_name=$tf
    fi
  
    _wanted files expl 'file from archive' _multi_parts / _tar_cache_list
  fi
}

v()
{
  set +x
  if [ $# = 1 ] ; then
    if [ -d "$1" ]; then
      cd $1
    else
      zless -rf $1 
    fi
  else
    if [ $(tar -tvf $1 $2 2> /dev/null | grep ' link to ' > /dev/null | wc -l) = 1 ]; then
      internal_path=$(tar -tvf $1 $2 2> /dev/null | grep ' link to ' > /dev/null | awk '{print $NF}')
    elif [ $(tar -tvf $1 $2 2> /dev/null | grep ' -> ' > /dev/null | wc -l) = 1 ]; then
      internal_path=$(tar -tvf $1 $2 2> /dev/null | grep ' -> ' > /dev/null | awk '{print $NF}')
    else
      internal_path=$2
    fi

    if [ -d "$internal_path" ]; then
      cd $internal_path
    else
      tar -Oxf $1 $internal_path
      # || zcat -f $1 | less -r
    fi
  fi
}
compdef _v v
