from scipy import stats
import random
import numpy as np
import plotly.express as px
import plotly.graph_objs as go
import plotly as py

numerical_html_files = []

categorical_html_files = []


def rand_web_color_hex():
    rgb = ""
    for _ in "RGB":
        i = random.randrange(0, 2 ** 8)
        rgb += i.to_bytes(1, "big").hex()
    return rgb


def generate_histogram_ploty(ds, name):
    fig = px.histogram(ds, x=name, color_discrete_sequence=['#' + rand_web_color_hex()],
                       labels=(dict(x=name.lower())), title="Histogram of " + name.lower())
    return fig


def generate_boxplot_ploty(ds, name):
    fig = px.box(ds, y=name, labels=(dict(y=name.lower())), title="Boxplot of " + name.lower())
    return fig


def generate_qqplot_ploty(x, name):
    qq = stats.probplot(x, dist='lognorm', sparams=(1))
    x = np.array([qq[0][0][0], qq[0][0][-1]])

    fig = go.Figure()
    fig.add_scatter(x=qq[0][0], y=qq[0][1], mode='markers')
    fig.add_scatter(x=x, y=qq[1][1] + qq[1][0] * x, mode='lines')
    fig.layout.update(showlegend=False)
    fig.update_layout(title="Q-Q Plot of " + name.lower(), xaxis_title="Theorical Quantiles",
                      yaxis_title="Sample Quantiles", )
    return fig


def generate_numeric_plots(ds, path):
    global numerical_html_files
    numerics = ['int64', 'float64']
    ds_numeric = ds.select_dtypes(include=numerics)
    for col in ds_numeric:
        col_numeric = ds_numeric[col].dropna()
        fig_histogram = generate_histogram_ploty(ds, col)
        fig_boxplot = generate_boxplot_ploty(ds, col)
        fig_qqplot = generate_qqplot_ploty(col_numeric, col)
        py.offline.plot(fig_histogram, filename=path + '/histogram_' + col + '.html')
        py.offline.plot(fig_boxplot, filename=path + '/boxplot_' + col + '.html')
        py.offline.plot(fig_qqplot, filename=path + '/qqplot_' + col + '.html')
        numerical_html_files.append(path + '/histogram_' + col + '.html')
        numerical_html_files.append(path + '/boxplot_' + col + '.html')
        numerical_html_files.append(path + '/qqplot_' + col + '.html')


def generate_categorical_plots_ploty(ds, path):
    global categorical_html_files
    ds_cat = ds.select_dtypes(include=['category'])
    for col in ds_cat:
        fig_barplot = generate_histogram_ploty(ds, col)
        py.offline.plot(fig_barplot, filename=path + '/barplot' + col + '.html')
        categorical_html_files.append(path + '/barplot' + col + '.html')


def generate_advanced_report(ds, name, path):
    generate_numeric_plots(ds, path)

    generate_categorical_plots_ploty(ds, path)

    global numerical_html_files, categorical_html_files

    if numerical_html_files or categorical_html_files:

        html_string = '''
            <html>
            <head>
                <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.1/css/bootstrap.min.css">
                <style>body{ margin:0 100; background:whitesmoke; }</style>
            </head>
            <body>
            <h1>Plots of dataframe ''' + name + ''' </h1>
            '''
        if numerical_html_files:
            html_string = html_string + '''<h2>Plots of numerical columns</h2>'''
            for file in numerical_html_files:
                html_string = html_string + '''
                    <iframe width="1000" height="550" frameborder="0" seamless="seamless" scrolling="no" 
                    src="
                    ''' + file + '''"></iframe>'''
        if categorical_html_files:
            html_string = html_string + '''
                <h2>Plots of categorical columns</h2>
                '''
            for file in categorical_html_files:
                html_string = html_string + '''
                    <iframe width="1000" height="550" frameborder="0" seamless="seamless" scrolling="no" 
                    src="
                    ''' + file + '''"></iframe>'''
        html_string = html_string + '''
            </body>
            </html>
            '''
        f = open(path + '/' + name + '_report.html', 'w')
        f.write(html_string)
        f.close()

        numerical_html_files = []

        categorical_html_files = []


    else:
        raise Exception("The dataset " + name + "no have columns of type 'category', 'int64' or 'float64' ")
