import subprocess
import json

ps_script = """
Add-Type -AssemblyName System.Speech
$synth = New-Object System.Speech.Synthesis.SpeechSynthesizer
$words = New-Object System.Collections.ArrayList
$synth.add_SpeakProgress({
    param($sender, $e)
    $words.Add([PSCustomObject]@{
        text = $e.Text
        audio_pos_ms = $e.AudioPosition.TotalMilliseconds
        char_pos = $e.CharacterPosition
        char_count = $e.CharacterCount
    }) | Out-Null
})
$synth.SetOutputToNull()
$synth.Speak('Welcome everyone to our investment committee meeting.')
$synth.Dispose()
$words | ConvertTo-Json
"""

res = subprocess.run(["powershell", "-NoProfile", "-Command", ps_script], capture_output=True, text=True)
print("Returncode:", res.returncode)
print("Stdout:", res.stdout[:500])
if res.stderr:
    print("Stderr:", res.stderr[:500])
