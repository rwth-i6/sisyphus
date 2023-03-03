# This imports all things typically used sisyphus
from sisyphus import *
from recipe import tools
from recipe import splitter


# Workflow example, it is possible to have multiple workflows in the same file. Since this function is named `main`
# you can call it by running sis manager config/workflow.main
async def main():
    # Initialize input data, tags are optional. The have no influence on the workflow,
    # but make it easier to track which input influenced a job
    input_data = tk.Path('input/lorem_upsum.txt', tags={'lorem_upsum'})

    # Create new lines after . and remove empty spaces at the beginning and end of each line
    sentences = tools.Pipeline(input_data, ["sed 's:\\.:\\n:g'", "sed 's:^ *::;s: *$::'"]).out

    # Count lines, lines is a Variable meaning once it's computed its value can be accessed using .get()
    # or by converting it into a string
    num_lines = tools.WordCount(sentences).lines

    # You can run computations on these variables even though they are not computed yet.
    # The requested computation is stored in a wrapper object and only resolved when .get() is called
    # or the object is converted into a string. An exception is raised if you call get() on an unfinished Variable
    # which doesn't have a backup value.
    middle = num_lines // 2
    first_half = tools.Head(sentences, middle).out

    # Tell Sisyphus that this output is needed and should be linked inside the output directory
    tk.register_output('first_half', first_half)
    tk.register_output('sentences', sentences)

    # Split each paragraph into a new line and again register output
    paragraphs_job = splitter.ParagraphSplitter(sentences)

    # paragraphs_job.outputs() is a coroutine, the `await` keyword will make sure that the
    # workflow will be stopped at this point and only continue once the paragraphs_job is finished
    paragraphs = await paragraphs_job.outputs()
    wc_per_paragraph = [tools.WordCount(paragraph) for paragraph in paragraphs]

    for i, wc in enumerate(wc_per_paragraph):
        # You can also register variables as output:
        tk.register_output('paragraph.%02i.words' % i, wc.words)

    # All jobs inside the wc_per_paragraph list will be computed after this line
    await tk.async_run(wc_per_paragraph)

    # We can now get the final value of all variables
    max_word_per_paragraph = max(wc.words.get() for wc in wc_per_paragraph)
    print(f"The largest paragraph has {max_word_per_paragraph} words")
