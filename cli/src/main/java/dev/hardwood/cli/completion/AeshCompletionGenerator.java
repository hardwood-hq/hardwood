package dev.hardwood.cli.completion;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class AeshCompletionGenerator {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: AeshCompletionGenerator <output-file>");
            System.exit(1);
        }

        String outputFile = args[0];
        String script = """
_hardwood() {
    local cur prev opts base
    COMPREPLY=()
    cur="${COMP_WORDS[COMP_CWORD]}"
    prev="${COMP_WORDS[COMP_CWORD-1]}"

    # Find the command being executed
    local i=1
    local cmd=""
    local subcmd=""
    while [ $i -lt $COMP_CWORD ]; do
        local s="${COMP_WORDS[i]}"
        case "$s" in
            info|schema|convert|footer|print|dive|inspect|help)
                cmd="$s"
                ;;
            columns|dictionary|pages|rowgroups)
                if [ "$cmd" = "inspect" ]; then
                    subcmd="$s"
                fi
                ;;
        esac
        i=$((i+1))
    done

    if [ -z "$cmd" ]; then
        if [[ "$cur" == -* ]]; then
            COMPREPLY=( $(compgen -W "-h --help -v --version" -- "$cur") )
        else
            COMPREPLY=( $(compgen -W "info schema convert footer print dive inspect help" -- "$cur") )
        fi
        return 0
    fi

    case "$cmd" in
        help)
            COMPREPLY=( $(compgen -W "info schema convert footer print dive inspect" -- "$cur") )
            return 0
            ;;
        info)
            COMPREPLY=( $(compgen -W "-f --file -h --help" -- "$cur") )
            return 0
            ;;
        schema)
            COMPREPLY=( $(compgen -W "-f --file -h --help" -- "$cur") )
            return 0
            ;;
        footer)
            COMPREPLY=( $(compgen -W "-f --file -h --help" -- "$cur") )
            return 0
            ;;
        convert)
            COMPREPLY=( $(compgen -W "-f --file --format -n --rows -c --columns -h --help" -- "$cur") )
            return 0
            ;;
        print)
            COMPREPLY=( $(compgen -W "-f --file -s --sample-size -w --max-width -t --truncate --no-truncate --transpose -i --row-index -d --row-delimiter -n --rows -c --columns -h --help" -- "$cur") )
            return 0
            ;;
        dive)
            COMPREPLY=( $(compgen -W "-f --file --max-dict-bytes --log-file -h --help" -- "$cur") )
            return 0
            ;;
        inspect)
            if [ -z "$subcmd" ]; then
                COMPREPLY=( $(compgen -W "columns dictionary pages rowgroups" -- "$cur") )
                return 0
            fi
            case "$subcmd" in
                columns)
                    COMPREPLY=( $(compgen -W "-f --file -h --help" -- "$cur") )
                    ;;
                dictionary)
                    COMPREPLY=( $(compgen -W "-f --file -c --column -l --limit -h --help" -- "$cur") )
                    ;;
                pages)
                    COMPREPLY=( $(compgen -W "-f --file -c --column --no-stats -h --help" -- "$cur") )
                    ;;
                rowgroups)
                    COMPREPLY=( $(compgen -W "-f --file -h --help" -- "$cur") )
                    ;;
            esac
            return 0
            ;;
    esac
}
complete -F _hardwood hardwood
""";

        try {
            Path path = Paths.get(outputFile);
            if (path.getParent() != null) {
                Files.createDirectories(path.getParent());
            }
            Files.writeString(path, script, StandardCharsets.UTF_8);
            System.out.println("Completion script generated successfully at: " + outputFile);
        } catch (IOException e) {
            System.err.println("Failed to write completion script: " + e.getMessage());
            System.exit(1);
        }
    }
}
