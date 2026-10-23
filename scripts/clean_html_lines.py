import re
import os

FILES = [
    'public-menu-base.html',
    'gastronomia-local1.html',
    'gastronomia-local2.html',
    'gastronomia-local3.html',
    'gastronomia-local4.html',
    'gastronomia-local5.html',
    'gastronomia-independiente.html',
    'gastronomia.html'
]

def clean_file_lines(fname):
    if not os.path.exists(fname):
        print(f"File {fname} not found.")
        return

    with open(fname, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        
    output_lines = []
    skipping = False
    skip_indent = -1
    
    # Regex to detect start of grid
    grid_start_re = re.compile(r'^(\s*)<div class="(discounts-grid|products-grid)">')
    
    for line in lines:
        if skipping:
            # Check for closing div with same indentation
            # We assume the closing div is on its own line: </div>
            # And has the exact same indentation string? Or just length?
            # Usually strict equality of whitespace is safest if formatted.
            
            # Check if line is just whitespace + </div> + whitespace
            stripped = line.strip()
            if stripped == '</div>':
                # Check indentation length
                current_indent = len(line) - len(line.lstrip())
                if current_indent == skip_indent:
                    # Found closing tag
                    output_lines.append(line)
                    skipping = False
                    continue
            
            # Continue skipping
            continue
            
        else:
            match = grid_start_re.match(line)
            if match:
                # Start skipping
                output_lines.append(line)
                skipping = True
                skip_indent = len(match.group(1))
            else:
                output_lines.append(line)
                
    with open(fname, 'w', encoding='utf-8') as f:
        f.writelines(output_lines)
    print(f"Cleaned {fname}")

def run():
    for f in FILES:
        clean_file_lines(f)

if __name__ == '__main__':
    run()
