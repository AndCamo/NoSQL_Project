// Query card selection
document.querySelectorAll('.query-card').forEach(card => {
    card.addEventListener('click', function () {
        const queryNum = this.getAttribute('data-query');

        document.querySelectorAll('.query-card').forEach(c => c.classList.remove('active'));
        this.classList.add('active');

        // Show corresponding form
        document.querySelectorAll('.form-section').forEach(f => f.classList.remove('active'));
        document.getElementById(`form-${queryNum}`).classList.add('active');

        // Hide results
        document.getElementById('results').classList.remove('active');

        setTimeout(() => {
            document.getElementById(`form-${queryNum}`).scrollIntoView({
                behavior: 'smooth',
                block: 'start'
            });
        }, 100);
    });
});

// Query 1 Form Handler
document.getElementById('query-form-1').addEventListener('submit', async function (e) {
    e.preventDefault();

    const personId = document.getElementById('param1-1').value;

    document.getElementById('loading').classList.add('active');

    try {
        console.log(`Calling Python backend with person ID: ${personId}`);
        const result = await eel.execute_query_1(personId)();
        if (result.state == 'error')
            return displayError(result.result)
        else
            console.log(result.result);
            displayQuery1Results(result.result)

    } catch (error) {
        console.error('Error executing query:', error);
        alert('An error occurred while executing the query.');
    } finally {
        document.getElementById('loading').classList.remove('active');
    }
});

// Query 2 Form Handler
document.getElementById('query-form-2').addEventListener('submit', async function (e) {
    e.preventDefault();

    const param1 = document.getElementById('param2-1').value;

    document.getElementById('loading').classList.add('active');

    try {
        const result = await eel.execute_query_2(param1)();
        if (result.state === 'error')
            return displayError(result.result)
        else {
            console.log(result.result);
            displayQuery1Results(result.result)
        }
    } catch (error) {
        console.error('Error executing query:', error);
        alert('An error occurred while executing the query.');
    } finally {
        document.getElementById('loading').classList.remove('active');
    }
});

// Query 3 Form Handler
document.getElementById('query-form-3').addEventListener('submit', async function (e) {
    e.preventDefault();

    document.getElementById('loading').classList.add('active');

    try {
        const result = await eel.execute_query_3()();
        if (result.state === 'error')
            return displayError(result.result)
        else {
            let title = "Most Influent Person"
            displaySingleGenericResult(result.result, title)
        }

    } catch (error) {
        console.error('Error executing query:', error);
        alert('An error occurred while executing the query.');
    } finally {
        document.getElementById('loading').classList.remove('active');
    }
});

// Query 4 Form Handler
document.getElementById('query-form-4').addEventListener('submit', async function (e) {
    e.preventDefault();

    const param1 = document.getElementById('param4-1').value;
    const param2 = document.getElementById('param4-2').value;

    document.getElementById('loading').classList.add('active');

    try {
        let begin_date = new Date(param1);
        let end_date = new Date(param2);

        if (begin_date > end_date) {
            alert("Begin date must be before end date.");
        }
        else {
            const result = await eel.execute_query_4(param1, param2)();

            if (result.state === 'error')
                return displayError(result.result)
            else{
                let title = `Most popular tag between ${param1} and ${param2}`
                displaySingleGenericResult(result.result, title)
            }


        }

    } catch (error) {
        console.error('Error executing query:', error);
        alert('An error occurred while executing the query.');
    } finally {
        document.getElementById('loading').classList.remove('active');
    }
});

// Query 5 Form Handler
document.getElementById('query-form-5').addEventListener('submit', async function (e) {
    e.preventDefault();

    const param1 = document.getElementById('param5-1').value;

    document.getElementById('loading').classList.add('active');

    try {
        const result = await eel.execute_query_5(Number(param1))();
        if (result.state === 'error')
            return displayError(result.result)
        else {
            let title = `Most influential person in University ${param1}`
            displaySingleGenericResult(result.result, title)
        }
    } catch (error) {
        console.error('Error executing query:', error);
        alert('An error occurred while executing the query.');
    } finally {
        document.getElementById('loading').classList.remove('active');
    }
});


// Display Query 1 Results
function displayQuery1Results(data) {
    const resultsContent = document.getElementById('results-content');
    let html = '';

    if (!(Array.isArray(data.University))){
        data.University = [data.University];

    }
    if (!(Array.isArray(data.Company))){
        data.Company = [data.Company];
    }

    // University Section
    if (data.University && data.University.length > 0) {
        html += '<div class="result-card">';
        html += '<h4><i class="fas fa-university"></i> University Information</h4>';
        data.University.forEach(uni => {
            html += '<div class="result-item">';
            for (let prop in uni){
                html += `<div><span class="result-label">${prop}:</span><span class="result-value">${uni[prop]}</span></div>`;
            }
            html += '</div>';
        });
        html += '</div>';
    }

    // Company Section
    if (data.Company && data.Company.length > 0) {
        html += '<div class="result-card">';
        html += '<h4><i class="fas fa-building"></i> Company Information</h4>';
        data.Company.forEach(company => {
            html += '<div class="result-item">';
            for (let prop in company){
                html += `<div><span class="result-label">${prop}:</span><span class="result-value">${company[prop]}</span></div>`;
            }
            html += '</div>';
        });
        html += '</div>';
    }

    resultsContent.innerHTML = html;
    document.getElementById('results').classList.add('active');

    setTimeout(() => {
        document.getElementById('results').scrollIntoView({
            behavior: 'smooth',
            block: 'start'
        });
    }, 100);
}

// Display Generic Results
function displaySingleGenericResult(data, title) {
    const resultsContent = document.getElementById('results-content');

    let html = '<div class="result-card">';
    html += `<h4><i class="fas fa-database"></i> ${title}</h4>`;
    html += '<div class="result-item">';
    for (let prop in data){
        // if the data is an object
        if (typeof data[prop] === 'object'){
            html += `<div><span class="result-label">${prop}</span>`;
            html += `<ul>`;
            for (let subProp in data[prop]){
                html += `<li>${subProp}: ${data[prop][subProp]}</li>`;
            }
            html += `</ul>`;
            html += `</div>`;
        }
        // if the data is a single value
        else {
            html += `<div><span class="result-label">${prop}:</span><span class="result-value">${data[prop]}</span></div>`;
        }
    }
    html += '</div>';
    html += '</div>';


    resultsContent.innerHTML = html;
    document.getElementById('results').classList.add('active');

    setTimeout(() => {
        document.getElementById('results').scrollIntoView({
            behavior: 'smooth',
            block: 'start'
        });
    }, 100);
}

function displayError(error) {
    const resultsContent = document.getElementById('results-content');
    resultsContent.innerHTML = `<div class="result-card"><h4><i class="fas fa-exclamation-triangle"></i> Error</h4><div class="result-item"><pre style="margin: 0; white-space: pre-wrap;">${error}</pre></div></div>`;

        document.getElementById('results').classList.add('active');

    setTimeout(() => {
        document.getElementById('results').scrollIntoView({
            behavior: 'smooth',
            block: 'start'
        });
    }, 100);
}