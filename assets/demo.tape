# VHS tape for the README hero demo.
# Regenerate with: vhs assets/demo.tape
#
# Shows the ez-cdc CLI opening and the interactive main menu.

Output assets/demo.gif

Require ez-cdc

Set Shell        "zsh"
Set FontSize     16
Set Width        1200
Set Height       780
Set Theme        "Dracula"
Set Padding      28
Set TypingSpeed  60ms
Set PlaybackSpeed 1.0

# Fresh prompt
Hide
Type "clear" Enter
Show

Sleep 500ms

Type "ez-cdc"
Sleep 400ms
Enter

# Wait for banner + menu to render
Sleep 3s

# Show the arrow keys actually work — cycle a couple options
Down
Sleep 600ms
Down
Sleep 600ms
Down
Sleep 800ms
Up
Sleep 600ms
Up
Sleep 800ms
Up
Sleep 1500ms

# Clean exit (Ctrl+C — cliclack intercepts it)
Ctrl+C
Sleep 800ms
